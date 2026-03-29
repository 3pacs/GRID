"""
GRID Intelligence — Global Commodities & Agriculture Power Network Map.

Static intelligence dossier on the global commodities and agriculture
power structure: grain traders (ABCD+), mining giants, precious metals,
agriculture technology monopolies, and food security dynamics.

All data sourced from public filings (10-K, DEF 14A, Form 4),
OpenSecrets, FAO, USDA, company annual reports, ICIJ leaks,
and credible journalism (FT, Reuters, Bloomberg).

Confidence labels per GRID convention:
    confirmed  — directly from SEC filings, annual reports, or government databases
    derived    — calculated from confirmed data
    estimated  — credible third-party estimate (OpenSecrets, SIPRI, Reuters, etc.)
    rumored    — reported in media but unverified
    inferred   — pattern-detected by GRID analysis

Data vintage: public information through early 2025.
Update frequency: refresh quarterly after earnings season + USDA WASDE reports.

Key entry points:
    get_commodities_network()          — full network dict
    get_entity(ticker_or_id)           — single entity dossier
    get_grain_traders()                — ABCD+ grain trader profiles
    get_mining_giants()                — major mining companies
    get_precious_metals()              — gold/silver/platinum producers + central bank reserves
    get_agtech_monopolies()            — agriculture technology + chemical monopolies
    get_food_security_signals()        — geopolitical food security risk signals
    get_commodities_lobbying_summary() — aggregated lobbying + PAC spend
"""

from __future__ import annotations

from typing import Any


def get_commodities_network() -> dict[str, Any]:
    """Return the full commodities & agriculture intelligence network."""
    return COMMODITIES_AGRICULTURE_NETWORK


def get_entity(ticker_or_id: str) -> dict[str, Any] | None:
    """Return dossier for a single entity by ticker or ID."""
    for section in (
        "commodity_traders", "mining_giants", "precious_metals_producers",
        "agtech_monopolies",
    ):
        entities = COMMODITIES_AGRICULTURE_NETWORK.get(section, {})
        if ticker_or_id in entities:
            return entities[ticker_or_id]
    return None


def get_grain_traders() -> list[dict[str, Any]]:
    """Return all ABCD+ grain trader profiles."""
    return list(COMMODITIES_AGRICULTURE_NETWORK.get("commodity_traders", {}).values())


def get_mining_giants() -> list[dict[str, Any]]:
    """Return all major mining company profiles."""
    return list(COMMODITIES_AGRICULTURE_NETWORK.get("mining_giants", {}).values())


def get_precious_metals() -> dict[str, Any]:
    """Return precious metals producers + central bank reserve data."""
    return {
        "producers": list(
            COMMODITIES_AGRICULTURE_NETWORK.get("precious_metals_producers", {}).values()
        ),
        "central_bank_reserves": COMMODITIES_AGRICULTURE_NETWORK.get(
            "central_bank_gold_reserves", {}
        ),
    }


def get_agtech_monopolies() -> list[dict[str, Any]]:
    """Return agriculture technology and chemical monopoly profiles."""
    return list(COMMODITIES_AGRICULTURE_NETWORK.get("agtech_monopolies", {}).values())


def get_food_security_signals() -> list[dict[str, Any]]:
    """Return active food security risk signals."""
    return COMMODITIES_AGRICULTURE_NETWORK.get("food_security", {}).get("risk_signals", [])


def get_commodities_lobbying_summary() -> dict[str, Any]:
    """Aggregate lobbying + PAC spend across all commodities entities."""
    total_lobbying = 0.0
    total_pac = 0.0
    for section in (
        "commodity_traders", "mining_giants", "precious_metals_producers",
        "agtech_monopolies",
    ):
        for _id, entity in COMMODITIES_AGRICULTURE_NETWORK.get(section, {}).items():
            total_lobbying += entity.get("lobbying", {}).get("annual_spend_usd", 0)
            total_pac += entity.get("pac_contributions", {}).get("total_2024_cycle_usd", 0)
    return {
        "total_annual_lobbying_usd": total_lobbying,
        "total_pac_2024_cycle_usd": total_pac,
        "confidence": "estimated",
        "source": "OpenSecrets aggregation",
    }


# ══════════════════════════════════════════════════════════════════════════
# STATIC INTELLIGENCE DATA
# ══════════════════════════════════════════════════════════════════════════

COMMODITIES_AGRICULTURE_NETWORK: dict[str, Any] = {
    "meta": {
        "report_type": "global_commodities_agriculture_power_network",
        "version": "1.0.0",
        "data_vintage": "2025-Q1",
        "refresh_cadence": "quarterly",
        "classification": "OSINT",
        "sources": [
            "SEC EDGAR (10-K, DEF 14A, Form 4, 13F)",
            "OpenSecrets.org",
            "UN FAO Food Price Index",
            "USDA WASDE Reports",
            "Company Annual Reports / Investor Presentations",
            "Reuters / Bloomberg / Financial Times",
            "ICIJ Offshore Leaks Database",
            "Senate Lobbying Disclosure Act filings",
            "DOJ FCPA / Plea Agreements",
            "World Gold Council",
            "USGS Mineral Commodity Summaries",
            "S&P Global Commodity Insights",
        ],
    },

    # ══════════════════════════════════════════════════════════════════
    # COMMODITY TRADERS (ABCD+ Grain Traders)
    # ══════════════════════════════════════════════════════════════════

    "commodity_traders": {

        # ── CARGILL ─────────────────────────────────────────────────
        "CARGILL": {
            "name": "Cargill, Incorporated",
            "ticker": None,
            "exchange": None,
            "public": False,
            "sector": "Agricultural Commodities / Food Processing / Trading",
            "revenue_fy2024_usd": 177_000_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "Cargill annual report FY2024 (June year-end)",
            "net_income_fy2024_usd": 2_480_000_000,
            "net_income_confidence": "confirmed",
            "employees": 160_000,
            "hq": "Wayzata, Minnesota",
            "founded": 1865,

            "ownership": {
                "structure": "private — family controlled",
                "controlling_families": ["Cargill family", "MacMillan family"],
                "family_ownership_pct": 88,
                "family_members_with_stake": "approximately 100 descendants of W.W. Cargill",
                "estimated_family_wealth_usd": 45_000_000_000,
                "wealth_confidence": "estimated",
                "wealth_source": "Forbes / Bloomberg Billionaires Index",
                "notes": (
                    "Largest private company in the US by revenue. The Cargill and "
                    "MacMillan families are among the wealthiest dynasties in America. "
                    "Company has resisted IPO for 160 years, providing zero public "
                    "disclosure requirements. Opaque structure shields trading "
                    "positions and profit margins from scrutiny."
                ),
                "confidence": "confirmed",
            },

            "ceo": {
                "name": "Brian Sikes",
                "title": "President & CEO",
                "appointed": "2023-01-01",
                "predecessor": "David MacLennan",
                "background": (
                    "30-year Cargill veteran. Rose through animal nutrition and "
                    "protein businesses. Succeeded David MacLennan who served "
                    "as CEO 2013-2022."
                ),
                "compensation": "not disclosed (private company)",
                "confidence": "confirmed",
            },

            "key_people": [
                {
                    "name": "David MacLennan",
                    "title": "Executive Chairman",
                    "role_detail": "Former CEO, now chairs board",
                    "confidence": "confirmed",
                },
                {
                    "name": "Marcel Smits",
                    "title": "CFO",
                    "confidence": "confirmed",
                },
                {
                    "name": "Pilar Cruz",
                    "title": "Chief Sustainability Officer",
                    "role_detail": "Leads deforestation response amid ESG pressure",
                    "confidence": "confirmed",
                },
            ],

            "key_family_members": [
                {
                    "name": "Pauline MacMillan Keinath",
                    "net_worth_usd": 12_500_000_000,
                    "relation": "Great-granddaughter of W.W. Cargill",
                    "confidence": "estimated",
                    "source": "Forbes",
                },
                {
                    "name": "Marion MacMillan Pictet",
                    "net_worth_usd": 8_600_000_000,
                    "relation": "Great-granddaughter of W.W. Cargill",
                    "confidence": "estimated",
                },
                {
                    "name": "Austen Cargill II",
                    "net_worth_usd": 6_300_000_000,
                    "relation": "Great-great-grandson of W.W. Cargill",
                    "confidence": "estimated",
                },
                {
                    "name": "James R. Cargill II",
                    "net_worth_usd": 6_300_000_000,
                    "relation": "Great-grandson of W.W. Cargill",
                    "confidence": "estimated",
                },
            ],

            "business_segments": [
                {
                    "segment": "Agricultural Supply Chain & Origination",
                    "description": "Grain, oilseed, cotton origination and trading globally",
                    "share_of_global_grain_trade_pct": 25,
                    "confidence": "estimated",
                },
                {
                    "segment": "Food Ingredients & Applications",
                    "description": "Starches, sweeteners, oils, cocoa, chocolate",
                    "confidence": "confirmed",
                },
                {
                    "segment": "Animal Nutrition & Protein",
                    "description": "Feed, aquaculture, poultry, beef processing",
                    "us_beef_market_share_pct": 22,
                    "confidence": "estimated",
                },
                {
                    "segment": "Industrial & Financial Services",
                    "description": "Energy trading, steel, risk management, trade finance",
                    "confidence": "confirmed",
                },
            ],

            "controversies": [
                {
                    "issue": "Amazon deforestation",
                    "detail": (
                        "Repeatedly linked to soy and cattle supply chains driving "
                        "deforestation in the Brazilian Amazon and Cerrado. Mighty Earth "
                        "and Greenpeace campaigns named Cargill as worst offender among "
                        "ABCD traders. Pledged zero-deforestation by 2030 but missed "
                        "earlier 2020 target."
                    ),
                    "confidence": "confirmed",
                    "source": "Mighty Earth, Greenpeace, Chain Reaction Research",
                },
                {
                    "issue": "Child labor in cocoa supply chain",
                    "detail": (
                        "Named in Nestle USA v. Doe Supreme Court case (2021) regarding "
                        "child labor on cocoa farms in Ivory Coast. Court ruled companies "
                        "could not be held liable under Alien Tort Statute, but underlying "
                        "labor practices remain documented."
                    ),
                    "confidence": "confirmed",
                    "source": "US Supreme Court, Nestle USA v. Doe (2021)",
                },
                {
                    "issue": "Record profits during food crisis",
                    "detail": (
                        "Posted record $6.68B net income in FY2023 during global food "
                        "price inflation, drawing criticism from food security advocates "
                        "and UN Special Rapporteur. Critics allege traders profited from "
                        "speculation while millions faced hunger."
                    ),
                    "confidence": "confirmed",
                    "source": "Cargill annual report FY2023, Reuters, UNCTAD",
                },
                {
                    "issue": "Market opacity",
                    "detail": (
                        "As a private company, Cargill discloses minimal financial data. "
                        "No SEC filings, no quarterly reports. Commodity positions are "
                        "invisible to regulators and markets. CFTC has limited visibility."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "offshore_structures": {
                "known_jurisdictions": ["Switzerland (Geneva trading hub)", "Singapore", "Netherlands", "Luxembourg"],
                "notes": (
                    "Geneva office is primary trading hub for global grain flows. "
                    "Swiss structure common among ABCD traders for tax efficiency "
                    "and regulatory arbitrage. Dutch and Luxembourg entities used "
                    "for European holding structures."
                ),
                "confidence": "confirmed",
                "source": "Corporate registry filings, FT reporting",
            },

            "lobbying": {
                "annual_spend_usd": 3_800_000,
                "year": 2023,
                "key_issues": [
                    "Farm Bill provisions",
                    "Biofuels mandates (RFS)",
                    "Trade policy (tariffs, USMCA)",
                    "ESG disclosure rules (opposing mandatory Scope 3)",
                    "Waters of the US (WOTUS) regulations",
                ],
                "confidence": "estimated",
                "source": "OpenSecrets.org / Senate LDA filings",
            },

            "political_connections": {
                "revolving_door": [
                    {
                        "name": "Greg Page",
                        "role": "Former Cargill CEO",
                        "government_connection": "Served on President's Export Council under Obama",
                        "confidence": "confirmed",
                    },
                ],
                "trade_policy_influence": (
                    "Cargill is a dominant voice in US agricultural trade policy. "
                    "Strongly supported USMCA, lobbied against China tariffs on "
                    "soybeans. CEO has direct access to USDA Secretary and USTR."
                ),
                "confidence": "estimated",
            },

            "trading_signals": [
                {
                    "signal": "Private company — no insider filings, no 13F, no short interest",
                    "implication": "Trade via supply chain partners: Bunge (BG), ADM, DE, MON",
                    "confidence": "inferred",
                },
                {
                    "signal": "Record grain trade volumes correlate with Ukraine conflict",
                    "implication": "Cargill benefits disproportionately from supply disruptions",
                    "confidence": "derived",
                },
                {
                    "signal": "US-China soybean trade normalization",
                    "implication": "Watch USDA export inspections data for Cargill flow signals",
                    "confidence": "inferred",
                },
            ],
        },

        # ── ARCHER DANIELS MIDLAND (ADM) ───────────────────────────
        "ADM": {
            "name": "Archer-Daniels-Midland Company",
            "ticker": "ADM",
            "exchange": "NYSE",
            "public": True,
            "sector": "Agricultural Commodities / Food Processing",
            "market_cap_usd": 24_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 85_500_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024",
            "net_income_2024_usd": 1_200_000_000,
            "net_income_confidence": "confirmed",
            "employees": 40_000,
            "hq": "Chicago, Illinois",
            "founded": 1902,

            "ceo": {
                "name": "Juan Luciano",
                "title": "Chairman & CEO",
                "appointed": "2015-01-01",
                "total_compensation_2023_usd": 23_100_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2024",
                "background": (
                    "Argentine-born chemical engineer. Joined ADM in 2011 from "
                    "Dow Chemical. Transformed ADM from pure grain trader to "
                    "nutrition-focused company. Under pressure since 2024 "
                    "accounting scandal."
                ),
                "confidence": "confirmed",
            },

            "key_people": [
                {
                    "name": "Vikram Luthar",
                    "title": "CFO (interim, then permanent)",
                    "role_detail": "Replaced fired CFO Ismael Roig in Jan 2024",
                    "confidence": "confirmed",
                },
                {
                    "name": "Ismael Roig",
                    "title": "Former VP & CFO",
                    "role_detail": (
                        "Placed on administrative leave January 2024 amid accounting "
                        "investigation. SEC and DOJ probes opened. Roig later terminated."
                    ),
                    "confidence": "confirmed",
                    "source": "ADM 8-K filing January 2024, SEC investigation",
                },
            ],

            "accounting_scandal_2024": {
                "trigger": (
                    "January 22, 2024: ADM announced CFO Ismael Roig placed on leave "
                    "pending investigation into accounting practices in the Nutrition "
                    "segment. Stock dropped 24% in one day, wiping ~$9B in market cap."
                ),
                "sec_investigation": True,
                "doj_investigation": True,
                "segment_affected": "Nutrition (health & wellness ingredients)",
                "issues_identified": [
                    "Intersegment transfer pricing manipulation",
                    "Revenue recognition timing in Nutrition segment",
                    "Internal controls material weakness declared",
                ],
                "stock_impact": {
                    "one_day_drop_pct": -24,
                    "market_cap_lost_usd": 9_000_000_000,
                    "date": "2024-01-22",
                },
                "restatement": (
                    "ADM restated FY2023 and prior period financials. Nutrition "
                    "segment profits were materially overstated through intercompany "
                    "pricing that inflated segment results."
                ),
                "confidence": "confirmed",
                "source": "ADM 8-K, 10-K/A, SEC filings, Bloomberg",
            },

            "business_segments": [
                {
                    "segment": "Ag Services & Oilseeds",
                    "revenue_usd": 57_000_000_000,
                    "description": "Grain origination, oilseed processing, trading, transportation",
                    "confidence": "confirmed",
                },
                {
                    "segment": "Carbohydrate Solutions",
                    "revenue_usd": 12_000_000_000,
                    "description": "Corn processing, sweeteners, starches, ethanol",
                    "confidence": "confirmed",
                },
                {
                    "segment": "Nutrition",
                    "revenue_usd": 7_200_000_000,
                    "description": "Human and animal nutrition, flavors, health ingredients",
                    "notes": "Segment at center of accounting scandal",
                    "confidence": "confirmed",
                },
            ],

            "controversies": [
                {
                    "issue": "2024 accounting fraud investigation",
                    "detail": "See accounting_scandal_2024 section",
                    "confidence": "confirmed",
                },
                {
                    "issue": "1996 lysine price-fixing",
                    "detail": (
                        "ADM paid $100M criminal fine for lysine and citric acid "
                        "price-fixing conspiracy. VP Mark Whitacre was FBI informant "
                        "(subject of 'The Informant!' film). Three executives imprisoned."
                    ),
                    "confidence": "confirmed",
                    "source": "DOJ criminal case, 7th Circuit Court",
                },
                {
                    "issue": "Deforestation links",
                    "detail": "Named alongside Cargill in Amazon/Cerrado soy deforestation",
                    "confidence": "confirmed",
                },
            ],

            "lobbying": {
                "annual_spend_usd": 3_200_000,
                "year": 2023,
                "key_issues": [
                    "Biofuels (RFS, sustainable aviation fuel credits)",
                    "Farm Bill",
                    "Trade policy",
                    "ESG disclosure rules",
                ],
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "pac_contributions": {
                "pac_name": "ADM PAC",
                "total_2024_cycle_usd": 680_000,
                "split_pct": {"republican": 60, "democrat": 40},
                "confidence": "estimated",
                "source": "OpenSecrets.org 2024 cycle",
            },

            "insider_trading": {
                "net_pattern_12mo": "net_selling",
                "total_sold_12mo_usd": 28_000_000,
                "total_bought_12mo_usd": 500_000,
                "notable_transactions": [
                    {
                        "insider": "Juan Luciano (CEO)",
                        "type": "sale",
                        "amount_usd": 12_000_000,
                        "period": "Q3-Q4 2023 (pre-scandal)",
                        "notes": "Sales occurred months before accounting issues surfaced",
                        "confidence": "confirmed",
                        "source": "SEC Form 4",
                    },
                ],
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_jurisdictions": ["Switzerland", "Singapore", "Bermuda", "Netherlands"],
                "notes": "Swiss and Singapore trading offices for global commodity flows",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {
                    "signal": "Accounting scandal overhang — SEC/DOJ investigations ongoing",
                    "implication": "Potential additional restatements, fines, or leadership changes",
                    "confidence": "confirmed",
                },
                {
                    "signal": "CEO insider selling pre-scandal",
                    "implication": "Form 4 timing raises questions, watch for SEC enforcement",
                    "confidence": "derived",
                },
                {
                    "signal": "Nutrition segment writedown risk",
                    "implication": "Goodwill impairment possible if investigation reveals deeper issues",
                    "confidence": "inferred",
                },
            ],
        },

        # ── BUNGE ──────────────────────────────────────────────────
        "BG": {
            "name": "Bunge Global SA",
            "ticker": "BG",
            "exchange": "NYSE",
            "public": True,
            "sector": "Agricultural Commodities / Food Processing",
            "market_cap_usd": 12_500_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 55_700_000_000,
            "revenue_confidence": "confirmed",
            "employees": 23_000,
            "hq": "Chesterfield, Missouri (redomiciled to Switzerland 2023)",
            "founded": 1818,

            "ceo": {
                "name": "Gregory A. Heckman",
                "title": "CEO",
                "appointed": "2019-01-01",
                "total_compensation_2023_usd": 16_800_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2024",
                "confidence": "confirmed",
            },

            "viterra_merger": {
                "target": "Viterra Limited",
                "seller": "Glencore (majority owner, ~50%)",
                "other_sellers": ["CPPIB (Canada Pension Plan, ~37%)", "BCI (~13%)"],
                "deal_value_usd": 18_000_000_000,
                "announced": "2023-06-13",
                "status": "regulatory approvals ongoing as of Q1 2025",
                "structure": "Bunge acquires Viterra; Glencore becomes ~18% Bunge shareholder",
                "regulatory_hurdles": [
                    "EU antitrust (European Commission Phase II review)",
                    "Canada Competition Bureau",
                    "China SAMR",
                    "Australian ACCC",
                ],
                "strategic_rationale": (
                    "Creates world's largest publicly-traded grain trader. "
                    "Combines Bunge's crush capacity with Viterra's Canadian/Australian "
                    "origination. Glencore's ~18% stake creates Glencore-Bunge power axis."
                ),
                "glencore_angle": (
                    "Glencore, already the world's largest commodity trader, gains "
                    "major influence over global grain flows via its Bunge stake. "
                    "Gary Nagle (Glencore CEO) has signaled this is a strategic, "
                    "not financial, investment."
                ),
                "confidence": "confirmed",
                "source": "SEC S-4, Bunge press releases, EU Commission filings",
            },

            "redomiciliation": {
                "from": "United States (New York → Missouri)",
                "to": "Switzerland",
                "effective": "2023-11-01",
                "rationale": (
                    "Tax optimization and alignment with global trading operations. "
                    "Switzerland offers favorable holding company tax treatment and "
                    "proximity to European grain markets."
                ),
                "confidence": "confirmed",
            },

            "lobbying": {
                "annual_spend_usd": 1_800_000,
                "year": 2023,
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "pac_contributions": {
                "pac_name": "Bunge North America PAC",
                "total_2024_cycle_usd": 320_000,
                "split_pct": {"republican": 55, "democrat": 45},
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "trading_signals": [
                {
                    "signal": "Viterra merger closing risk — EU Phase II antitrust review",
                    "implication": "Regulatory delay or divestiture requirements could hit stock",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Glencore as ~18% shareholder post-close",
                    "implication": "Glencore influence over grain market through Bunge position",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Swiss redomiciliation — tax arbitrage play",
                    "implication": "Lower effective tax rate boosts EPS but may invite scrutiny",
                    "confidence": "derived",
                },
            ],
        },

        # ── LOUIS DREYFUS ──────────────────────────────────────────
        "LOUIS_DREYFUS": {
            "name": "Louis Dreyfus Company B.V.",
            "ticker": None,
            "exchange": None,
            "public": False,
            "sector": "Agricultural Commodities / Food Processing",
            "revenue_2024_usd": 48_000_000_000,
            "revenue_confidence": "estimated",
            "revenue_source": "Company annual results (limited disclosure)",
            "employees": 18_000,
            "hq": "Rotterdam, Netherlands (operational HQ: Geneva, Switzerland)",
            "founded": 1851,

            "ownership": {
                "structure": "private — family + sovereign wealth",
                "controlling_shareholder": "Margarita Louis-Dreyfus (via Akira BV)",
                "ownership_pct": 96,
                "minority_shareholder": "Abu Dhabi's ADQ (~4% via 2023 investment)",
                "estimated_family_wealth_usd": 9_000_000_000,
                "wealth_confidence": "estimated",
                "wealth_source": "Forbes / Bloomberg",
                "confidence": "confirmed",
            },

            "key_people": [
                {
                    "name": "Margarita Louis-Dreyfus",
                    "title": "Chairwoman of Supervisory Board",
                    "background": (
                        "Russian-born Swiss billionaire. Widow of Robert Louis-Dreyfus "
                        "(former Adidas CEO and LDC chairman). Controls company through "
                        "Akira BV holding. One of the wealthiest women in the world. "
                        "Also majority owner of Olympique de Marseille football club."
                    ),
                    "net_worth_usd": 9_000_000_000,
                    "confidence": "estimated",
                },
                {
                    "name": "Michael Gelchie",
                    "title": "CEO",
                    "appointed": "2016",
                    "background": "Former Louis Dreyfus trader, ex-Rabobank commodities",
                    "confidence": "confirmed",
                },
            ],

            "offshore_structures": {
                "known_jurisdictions": [
                    "Netherlands (holding company BV)",
                    "Switzerland (Geneva trading hub)",
                    "Singapore",
                    "Luxembourg",
                ],
                "notes": (
                    "Dutch BV structure for holding, Geneva for trading operations. "
                    "Like other ABCD traders, structured for minimal tax and "
                    "regulatory transparency."
                ),
                "confidence": "confirmed",
            },

            "controversies": [
                {
                    "issue": "Family control dispute",
                    "detail": (
                        "Margarita Louis-Dreyfus fought for years to consolidate control "
                        "after husband Robert's death in 2009. Resolved by buying out "
                        "other family branches. Her children (born via surrogate in 2015) "
                        "are eventual heirs."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "issue": "Commodity market manipulation allegations",
                    "detail": (
                        "Louis Dreyfus has faced periodic allegations of market "
                        "manipulation in sugar and coffee futures. CFTC investigated "
                        "in the 2010s; no charges resulted."
                    ),
                    "confidence": "rumored",
                },
            ],

            "trading_signals": [
                {
                    "signal": "Private — no market-tradeable securities",
                    "implication": "Monitor via commodity market flows and trade partner stocks",
                    "confidence": "confirmed",
                },
                {
                    "signal": "ADQ sovereign wealth investment (2023)",
                    "implication": "Abu Dhabi aligning food security strategy with ABCD access",
                    "confidence": "derived",
                },
            ],
        },

        # ── COFCO ──────────────────────────────────────────────────
        "COFCO": {
            "name": "COFCO International Ltd. / COFCO Corporation",
            "ticker": None,
            "exchange": None,
            "public": False,
            "sector": "Agricultural Commodities / State-Owned Enterprise",
            "revenue_2024_usd": 80_000_000_000,
            "revenue_confidence": "estimated",
            "revenue_source": "Parent COFCO Corp annual report (combined)",
            "employees": 100_000,
            "hq": "Beijing, China (intl HQ: Geneva, Switzerland)",
            "founded": 1949,

            "ownership": {
                "structure": "Chinese state-owned enterprise (SOE)",
                "parent": "COFCO Corporation (中粮集团)",
                "ultimate_owner": "State-owned Assets Supervision and Administration Commission (SASAC)",
                "notes": (
                    "COFCO is China's largest food and agriculture SOE. COFCO International "
                    "was created via acquisitions of Nidera (Dutch grain trader, 2014) and "
                    "Noble Agri (Hong Kong, 2014) for combined ~$3B. China's bid to break "
                    "ABCD dominance of global grain trade."
                ),
                "confidence": "confirmed",
            },

            "key_people": [
                {
                    "name": "Lu Jun",
                    "title": "Chairman, COFCO Corporation",
                    "background": "CPC-appointed chairman, career state enterprise executive",
                    "confidence": "confirmed",
                },
                {
                    "name": "Maxime Baudin",
                    "title": "CEO, COFCO International",
                    "background": "Former Louis Dreyfus executive",
                    "confidence": "confirmed",
                },
            ],

            "strategic_significance": {
                "geopolitical_role": (
                    "COFCO is the primary vehicle for China's food security strategy. "
                    "Beijing uses COFCO to secure soybean, grain, and oilseed supply "
                    "chains independent of ABCD traders. The 2014 acquisitions (Nidera, "
                    "Noble Agri) gave COFCO origination capability in Brazil, Argentina, "
                    "and Black Sea region."
                ),
                "food_security_angle": (
                    "China imports ~100M tons of soybeans annually (~60% of global trade). "
                    "COFCO is Beijing's insurance against Western traders using food as "
                    "geopolitical leverage, analogous to CNOOC's role in oil."
                ),
                "belt_and_road": (
                    "COFCO investments align with Belt and Road Initiative — acquiring "
                    "port, storage, and processing infrastructure in key exporting nations "
                    "(Brazil, Argentina, Ukraine, Romania, Australia)."
                ),
                "confidence": "derived",
            },

            "controversies": [
                {
                    "issue": "Nidera acquisition integration failures",
                    "detail": (
                        "COFCO's 2014 acquisition of Dutch grain trader Nidera resulted "
                        "in $150M+ losses due to unauthorized trading positions and "
                        "integration chaos. Several Nidera executives departed."
                    ),
                    "confidence": "confirmed",
                    "source": "Financial Times, Bloomberg",
                },
                {
                    "issue": "State-backed unfair competition",
                    "detail": (
                        "Western grain traders allege COFCO benefits from below-market "
                        "state financing (China Development Bank, Export-Import Bank of China) "
                        "and implicit government guarantees."
                    ),
                    "confidence": "estimated",
                },
            ],

            "trading_signals": [
                {
                    "signal": "State-owned — not directly tradeable",
                    "implication": "Monitor China soybean import data and COFCO's trade partners",
                    "confidence": "confirmed",
                },
                {
                    "signal": "China food security stockpiling",
                    "implication": (
                        "Watch USDA attaché reports on Chinese state reserve purchases. "
                        "COFCO buying sprees precede food price spikes."
                    ),
                    "confidence": "derived",
                },
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════
    # MINING GIANTS
    # ══════════════════════════════════════════════════════════════════

    "mining_giants": {

        # ── BHP ────────────────────────────────────────────────────
        "BHP": {
            "name": "BHP Group Limited",
            "ticker": "BHP",
            "exchange": "ASX (primary), NYSE (ADR)",
            "sector": "Diversified Mining",
            "market_cap_usd": 150_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_fy2024_usd": 55_700_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "BHP Annual Report FY2024 (June year-end)",
            "net_income_fy2024_usd": 13_700_000_000,
            "net_income_confidence": "confirmed",
            "employees": 80_000,
            "hq": "Melbourne, Australia",
            "founded": 1885,

            "listing_structure": {
                "note": (
                    "BHP unified its dual-listed company structure (BHP Group Ltd + "
                    "BHP Group Plc) in January 2022, ending the Australia/UK dual listing. "
                    "Now a single ASX-listed entity with NYSE ADR."
                ),
                "confidence": "confirmed",
            },

            "ceo": {
                "name": "Mike Henry",
                "title": "CEO",
                "appointed": "2020-01-01",
                "total_compensation_fy2024_usd": 11_200_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "BHP Annual Report FY2024",
                "background": (
                    "Canadian-born, career BHP executive since 2003. Rose through "
                    "coal, minerals Australia, and operations. Pivoted strategy toward "
                    "'future-facing' commodities (copper, nickel, potash) and away "
                    "from thermal coal and petroleum."
                ),
                "confidence": "confirmed",
            },

            "key_assets": [
                {
                    "asset": "Escondida (Chile)",
                    "type": "copper",
                    "ownership_pct": 57.5,
                    "partners": ["Rio Tinto (30%)", "JECO (12.5%)"],
                    "notes": "World's largest copper mine. ~5% of global copper supply.",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Western Australia Iron Ore (WAIO)",
                    "type": "iron_ore",
                    "ownership_pct": 85,
                    "production_mtpa": 280,
                    "notes": "Pilbara operations, one of world's largest iron ore systems",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Jansen Potash (Saskatchewan, Canada)",
                    "type": "potash",
                    "ownership_pct": 100,
                    "capex_usd": 5_700_000_000,
                    "status": "under construction, first production ~2026",
                    "notes": "BHP's bet on future food demand — enormous potash resource",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Nickel West (Australia)",
                    "type": "nickel",
                    "ownership_pct": 100,
                    "status": "under review — suspended operations 2024 due to low nickel prices",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Olympic Dam (South Australia)",
                    "type": "copper_gold_uranium",
                    "ownership_pct": 100,
                    "notes": "World's largest known uranium deposit, plus copper and gold",
                    "confidence": "confirmed",
                },
            ],

            "major_deals": [
                {
                    "deal": "Anglo American takeover attempt",
                    "year": 2024,
                    "detail": (
                        "BHP made three unsolicited offers for Anglo American (total "
                        "value up to ~$49B). Anglo rejected all, requiring BHP to first "
                        "demerge Anglo's South African assets (Kumba iron ore, Anglo "
                        "American Platinum). BHP walked away in May 2024 after final "
                        "offer lapsed. Would have created world's largest copper miner."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "deal": "OZ Minerals acquisition",
                    "year": 2023,
                    "value_usd": 6_400_000_000,
                    "detail": "Copper-gold mine acquisition (Prominent Hill, Carrapateena)",
                    "confidence": "confirmed",
                },
            ],

            "controversies": [
                {
                    "issue": "Samarco dam disaster (2015)",
                    "detail": (
                        "Samarco Mineração (50/50 BHP-Vale JV) tailings dam collapsed "
                        "in Mariana, Minas Gerais, Brazil. 19 people killed, 600km of "
                        "Rio Doce contaminated. Worst environmental disaster in Brazilian "
                        "history at the time. BHP faces ongoing litigation in both Brazil "
                        "and UK (largest group claim in English legal history, ~620,000 "
                        "claimants). Settlement negotiations ongoing as of 2025."
                    ),
                    "estimated_liability_usd": 30_000_000_000,
                    "confidence": "confirmed",
                    "source": "BHP Annual Report, UK High Court filings",
                },
            ],

            "lobbying": {
                "annual_spend_usd": 1_400_000,
                "year": 2023,
                "key_issues": [
                    "Critical minerals policy (US IRA implications)",
                    "Australian mining tax policy",
                    "Climate disclosure frameworks",
                ],
                "confidence": "estimated",
                "source": "OpenSecrets.org (US operations), Australian lobbying disclosures",
            },

            "trading_signals": [
                {
                    "signal": "Copper pivot strategy — 'future-facing commodities'",
                    "implication": "BHP is a leveraged bet on copper demand (EVs, AI data centers)",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Anglo American bid failure",
                    "implication": "BHP still acquisitive — watch for next copper target",
                    "confidence": "inferred",
                },
                {
                    "signal": "Samarco/Mariana liability overhang",
                    "implication": "$30B+ potential settlement is material to market cap",
                    "confidence": "confirmed",
                },
            ],
        },

        # ── RIO TINTO ──────────────────────────────────────────────
        "RIO": {
            "name": "Rio Tinto Group",
            "ticker": "RIO",
            "exchange": "ASX (primary), LSE, NYSE (ADR)",
            "sector": "Diversified Mining",
            "market_cap_usd": 105_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 53_700_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2024_usd": 11_800_000_000,
            "net_income_confidence": "confirmed",
            "employees": 57_000,
            "hq": "London, UK / Melbourne, Australia (dual HQ)",
            "founded": 1873,

            "ceo": {
                "name": "Jakob Stausholm",
                "title": "CEO",
                "appointed": "2021-01-01",
                "total_compensation_2023_usd": 9_800_000,
                "compensation_confidence": "confirmed",
                "background": (
                    "Danish national. Former CFO of Rio Tinto, career in A.P. Moller-Maersk "
                    "and ISS. Appointed CEO after predecessor Jean-Sébastien Jacques was "
                    "forced to resign over the Juukan Gorge destruction scandal."
                ),
                "confidence": "confirmed",
            },

            "juukan_gorge_scandal": {
                "event": (
                    "On May 24, 2020, Rio Tinto detonated explosives that destroyed "
                    "two 46,000-year-old Aboriginal rock shelters at Juukan Gorge in "
                    "Western Australia's Pilbara region, to expand an iron ore mine. "
                    "The shelters had evidence of continuous human habitation for 46,000 "
                    "years — among the oldest in Australia."
                ),
                "consequences": [
                    "CEO Jean-Sébastien Jacques forced to resign (Sep 2020)",
                    "Head of iron ore Chris Salisbury departed",
                    "Head of corporate relations Simone Niven departed",
                    "Australian Parliamentary inquiry",
                    "New Western Australian Aboriginal Cultural Heritage Act passed 2021",
                    "Board chair Simon Thompson did not seek re-election",
                ],
                "remediation": (
                    "Rio Tinto has undertaken extensive engagement with Puutu Kunti "
                    "Kurrama and Pinikura (PKKP) people. Cannot undo destruction."
                ),
                "confidence": "confirmed",
                "source": "Australian Parliamentary Joint Standing Committee report, Rio Tinto statements",
            },

            "key_assets": [
                {
                    "asset": "Pilbara Iron Ore (Western Australia)",
                    "type": "iron_ore",
                    "production_mtpa": 330,
                    "notes": "Largest iron ore operation globally. Low-cost Pilbara assets.",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Resolution Copper (Arizona, US)",
                    "type": "copper",
                    "ownership_pct": 55,
                    "partner": "BHP (45%)",
                    "status": "permitting — highly contested by Apache/San Carlos Apache tribes",
                    "notes": "One of world's largest undeveloped copper deposits. Blocked by Biden EPA.",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Oyu Tolgoi (Mongolia)",
                    "type": "copper_gold",
                    "ownership_pct": 66,
                    "partner": "Government of Mongolia (34%)",
                    "notes": "Underground expansion now producing — world-class copper-gold deposit",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Simandou (Guinea)",
                    "type": "iron_ore",
                    "ownership_pct": 45,
                    "partners": ["Winning Consortium (Chinese-backed, owns blocks 1&2)", "Government of Guinea"],
                    "notes": (
                        "World's largest untapped iron ore deposit. Infrastructure cost >$15B. "
                        "Decades of delays, political instability, and corruption allegations."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "controversies": [
                {
                    "issue": "Juukan Gorge destruction",
                    "detail": "See juukan_gorge_scandal section",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Bougainville mine (Papua New Guinea)",
                    "detail": (
                        "Rio Tinto's Panguna copper mine operations contributed to civil war "
                        "in Bougainville (1988-1998). ~20,000 deaths. Mine remains closed. "
                        "Bougainville voted 98% for independence in 2019 referendum."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "issue": "Simandou corruption",
                    "detail": (
                        "Former Rio Tinto executives accused of paying bribes to secure "
                        "mining rights in Guinea's Simandou iron ore deposit. BSGR and "
                        "VALE also implicated. FBI and UK SFO investigations."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "lobbying": {
                "annual_spend_usd": 1_200_000,
                "year": 2023,
                "confidence": "estimated",
                "source": "OpenSecrets.org (US), Australian lobbying disclosures",
            },

            "trading_signals": [
                {
                    "signal": "Iron ore China dependency — ~60% revenue from China",
                    "implication": "RIO is a proxy for Chinese steel demand and property sector",
                    "confidence": "derived",
                },
                {
                    "signal": "Copper growth pipeline (Oyu Tolgoi, Resolution)",
                    "implication": "Transition from iron ore to copper-weighted portfolio",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Simandou first ore target ~2025-2026",
                    "implication": "New supply could depress iron ore prices if China demand weakens",
                    "confidence": "estimated",
                },
            ],
        },

        # ── VALE ───────────────────────────────────────────────────
        "VALE": {
            "name": "Vale S.A.",
            "ticker": "VALE",
            "exchange": "B3 (São Paulo), NYSE (ADR)",
            "sector": "Mining (Iron Ore dominant)",
            "market_cap_usd": 45_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 41_800_000_000,
            "revenue_confidence": "confirmed",
            "employees": 67_000,
            "hq": "Rio de Janeiro, Brazil",
            "founded": 1942,

            "ceo": {
                "name": "Eduardo Bartolomeo",
                "title": "CEO",
                "appointed": "2019-03",
                "background": (
                    "Appointed in the aftermath of Brumadinho dam disaster. "
                    "Career Vale executive. Task: rebuild reputation and resolve "
                    "massive environmental liabilities."
                ),
                "confidence": "confirmed",
            },

            "dam_disasters": {
                "brumadinho_2019": {
                    "date": "2019-01-25",
                    "location": "Brumadinho, Minas Gerais, Brazil",
                    "deaths": 270,
                    "detail": (
                        "Tailings dam collapse at Córrego do Feijão mine killed 270 people, "
                        "most of them Vale employees eating lunch in the cafeteria below "
                        "the dam. TÜV SÜD (German auditor) had certified the dam as stable "
                        "just months before. Criminal charges filed against Vale executives "
                        "and TÜV SÜD engineers."
                    ),
                    "settlement_usd": 7_000_000_000,
                    "criminal_charges": True,
                    "confidence": "confirmed",
                },
                "mariana_samarco_2015": {
                    "date": "2015-11-05",
                    "location": "Mariana, Minas Gerais, Brazil",
                    "deaths": 19,
                    "detail": "Samarco JV with BHP. See BHP entry for details.",
                    "estimated_total_liability_usd": 30_000_000_000,
                    "confidence": "confirmed",
                },
            },

            "key_assets": [
                {
                    "asset": "Carajás (Pará, Brazil)",
                    "type": "iron_ore",
                    "production_mtpa": 190,
                    "notes": (
                        "World's largest iron ore mine complex. Produces highest-grade "
                        "iron ore (66%+ Fe) in commercial volumes. Strategic advantage "
                        "as steel industry shifts to higher-grade feedstock for emissions."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "asset": "Nickel operations (Indonesia, Canada, New Caledonia)",
                    "type": "nickel",
                    "notes": "Major nickel producer; Indonesian operations growing rapidly",
                    "confidence": "confirmed",
                },
            ],

            "controversies": [
                {
                    "issue": "Brumadinho dam collapse (2019)",
                    "detail": "See dam_disasters section",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Mariana/Samarco dam collapse (2015)",
                    "detail": "See dam_disasters section",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Deforestation in Carajás region",
                    "detail": "Vale's railway and mining operations linked to Amazon deforestation",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Indigenous community displacement",
                    "detail": (
                        "Expansion of Carajás complex has displaced multiple indigenous "
                        "communities. Ongoing legal disputes with Xikrin, Kayapó, and "
                        "other indigenous peoples."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "Iron ore China dependency (~65% of revenue)",
                    "implication": "VALE is the highest-beta play on Chinese construction demand",
                    "confidence": "derived",
                },
                {
                    "signal": "Dam disaster liability overhang",
                    "implication": "Brumadinho + Mariana total liabilities >$37B — overhang until resolved",
                    "confidence": "confirmed",
                },
                {
                    "signal": "High-grade iron ore premium widening",
                    "implication": "Carajás 66%+ Fe ore benefits from decarbonization trends in steel",
                    "confidence": "derived",
                },
            ],
        },

        # ── FREEPORT-MCMORAN (FCX) ────────────────────────────────
        "FCX": {
            "name": "Freeport-McMoRan Inc.",
            "ticker": "FCX",
            "exchange": "NYSE",
            "sector": "Copper Mining",
            "market_cap_usd": 60_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 22_800_000_000,
            "revenue_confidence": "confirmed",
            "employees": 27_800,
            "hq": "Phoenix, Arizona",
            "founded": 1912,

            "ceo": {
                "name": "Richard C. Adkerson",
                "title": "Chairman & CEO",
                "appointed": "2003 (CEO), 2021 (Chairman)",
                "total_compensation_2023_usd": 23_500_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2024",
                "background": (
                    "CPA and finance background. Has led Freeport through multiple "
                    "commodity cycles, the disastrous oil/gas diversification, and "
                    "complex Indonesia negotiations. One of the longest-tenured mining CEOs."
                ),
                "confidence": "confirmed",
            },

            "key_assets": [
                {
                    "asset": "Grasberg (Papua, Indonesia)",
                    "type": "copper_gold",
                    "ownership_pct": 48.76,
                    "partner": "PT Indonesia Asahan Aluminium / Indonesian government (51.24%)",
                    "notes": (
                        "World's largest gold mine and second-largest copper mine. "
                        "Underground block cave operation (one of the largest in the world). "
                        "Indonesia renegotiated ownership in 2018, forcing Freeport to divest "
                        "majority to state-owned Inalum. Freeport retained operating control."
                    ),
                    "annual_copper_production_lbs": 1_600_000_000,
                    "annual_gold_production_oz": 1_800_000,
                    "confidence": "confirmed",
                },
                {
                    "asset": "Morenci (Arizona, US)",
                    "type": "copper",
                    "ownership_pct": 72,
                    "notes": "Largest copper mine in North America",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Cerro Verde (Peru)",
                    "type": "copper",
                    "ownership_pct": 53.56,
                    "confidence": "confirmed",
                },
            ],

            "indonesia_political_risk": {
                "divestiture_history": (
                    "Indonesia mandated 51% local ownership of mining operations. "
                    "Freeport negotiated for years, ultimately selling 51.24% of Grasberg "
                    "to state-owned Inalum in 2018 for $3.85B. Freeport retained "
                    "management and operating control through 2041."
                ),
                "export_ban_risk": (
                    "Indonesia periodically threatens mineral export bans to force "
                    "domestic smelting. Freeport building $3B smelter in Gresik, Java. "
                    "Export ban is a key risk for copper concentrate shipments."
                ),
                "political_connections": (
                    "Freeport's operations in Papua are deeply politically sensitive. "
                    "Papuan separatist movement (OPM) has targeted mine infrastructure. "
                    "Indonesian military provides mine security — creating human rights "
                    "concerns documented by Amnesty International and Yale Law."
                ),
                "confidence": "confirmed",
                "source": "FCX 10-K, Amnesty International, Reuters",
            },

            "controversies": [
                {
                    "issue": "Environmental destruction at Grasberg",
                    "detail": (
                        "Riverine tailings disposal has deposited billions of tons of mine "
                        "waste into the Ajkwa/Aikwa river system in Papua. Satellite imagery "
                        "shows massive environmental damage downstream. One of the world's "
                        "most environmentally destructive mining operations."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "issue": "Human rights in Papua",
                    "detail": (
                        "Indonesian military security around Grasberg implicated in human "
                        "rights abuses against indigenous Amungme and Kamoro peoples. "
                        "Freeport has paid military/police security payments."
                    ),
                    "confidence": "confirmed",
                    "source": "Global Witness, Amnesty International",
                },
                {
                    "issue": "Oil & gas diversification disaster (2012-2016)",
                    "detail": (
                        "FCX acquired Plains Exploration ($6.9B) and McMoRan Exploration "
                        "($3.7B) in 2012-2013 at oil price peak. Wrote down $13B+ when "
                        "oil crashed. Carl Icahn forced board changes."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "lobbying": {
                "annual_spend_usd": 2_100_000,
                "year": 2023,
                "key_issues": [
                    "Critical minerals policy (IRA, CHIPS Act implications)",
                    "Mining permitting reform",
                    "Indonesia trade relations",
                    "EPA Clean Air Act compliance",
                ],
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "pac_contributions": {
                "pac_name": "Freeport-McMoRan Inc. PAC",
                "total_2024_cycle_usd": 450_000,
                "split_pct": {"republican": 65, "democrat": 35},
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "trading_signals": [
                {
                    "signal": "Copper is the new oil — electrification supercycle thesis",
                    "implication": "FCX is the highest-leverage US-listed pure copper play",
                    "confidence": "derived",
                },
                {
                    "signal": "Grasberg underground ramp-up complete",
                    "implication": "Cash flow inflection — Grasberg now in peak production phase",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Indonesia smelter mandate",
                    "implication": "Export ban risk if Gresik smelter delayed",
                    "confidence": "confirmed",
                },
            ],
        },

        # ── GLENCORE ───────────────────────────────────────────────
        "GLEN": {
            "name": "Glencore plc",
            "ticker": "GLEN",
            "exchange": "LSE (primary), JSE",
            "sector": "Commodity Trading + Mining",
            "market_cap_usd": 60_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 217_000_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "Glencore Annual Report 2024",
            "net_income_2024_usd": 4_300_000_000,
            "net_income_confidence": "confirmed",
            "employees": 152_000,
            "hq": "Baar, Switzerland",
            "founded": "1974 (as Marc Rich + Co AG)",

            "ceo": {
                "name": "Gary Nagle",
                "title": "CEO",
                "appointed": "2021-07-01",
                "total_compensation_2023_usd": 9_200_000,
                "compensation_confidence": "confirmed",
                "background": (
                    "South African. Career Glencore executive since 2000. Rose through "
                    "coal operations. Took over from Ivan Glasenberg, the legendary "
                    "trader who built modern Glencore."
                ),
                "confidence": "confirmed",
            },

            "key_people": [
                {
                    "name": "Ivan Glasenberg",
                    "title": "Former CEO (2002-2021)",
                    "role_detail": (
                        "Still significant shareholder (~9% stake). Built Glencore from "
                        "Marc Rich's trading firm into world's largest commodity trader. "
                        "Engineered 2011 IPO and 2013 Xstrata merger. South African, "
                        "Swiss/Australian citizen. Net worth ~$10B."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "name": "Marc Rich",
                    "title": "Founder (deceased 2013)",
                    "role_detail": (
                        "Founded Marc Rich + Co in 1974. Indicted by US DOJ in 1983 for "
                        "tax evasion, racketeering, and illegal trading with Iran during "
                        "hostage crisis. Fled to Switzerland. Controversially pardoned by "
                        "President Clinton on his last day in office (Jan 20, 2001). "
                        "Eric Holder (later AG) facilitated pardon. Rich sold company to "
                        "management (led by Glasenberg) in 1994."
                    ),
                    "confidence": "confirmed",
                    "source": "DOJ records, Presidential pardon records",
                },
            ],

            "doj_plea_2022": {
                "plea_date": "2022-05-24",
                "total_penalties_usd": 1_100_000_000,
                "charges": [
                    "Bribery of foreign officials (FCPA)",
                    "Market manipulation of oil benchmarks",
                ],
                "countries_bribed": [
                    "Nigeria", "Cameroon", "Ivory Coast", "Equatorial Guinea",
                    "Brazil", "Venezuela", "DRC",
                ],
                "detail": (
                    "Glencore pleaded guilty to FCPA bribery charges and commodity "
                    "price manipulation. DOJ found Glencore paid over $100M in bribes "
                    "to government officials in at least 7 countries to secure oil "
                    "contracts and gain favorable treatment. Additional $500M+ in "
                    "penalties from UK SFO, Brazilian authorities, and Swiss AG."
                ),
                "monitor": "Three-year compliance monitor imposed by DOJ",
                "confidence": "confirmed",
                "source": "DOJ press release, plea agreement, UK SFO",
            },

            "business_model": {
                "description": (
                    "Unique hybrid: massive industrial mining + world's largest commodity "
                    "trading operation. Trading arm provides physical market intelligence "
                    "that informs mining investment decisions and vice versa. Critics call "
                    "this structural inside information."
                ),
                "trading_revenue_usd": 180_000_000_000,
                "mining_revenue_usd": 37_000_000_000,
                "confidence": "estimated",
            },

            "bunge_viterra_deal": {
                "detail": "Selling Viterra to Bunge for ~$18B. See Bunge entry.",
                "resulting_bunge_stake_pct": 18,
                "strategic_significance": (
                    "Post-deal, Glencore will control ~18% of the world's largest "
                    "public grain trader while retaining its metals, energy, and coal "
                    "trading dominance. Cross-commodity intelligence advantage."
                ),
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_jurisdictions": [
                    "Switzerland (HQ — Baar, Zug canton — low tax)",
                    "Jersey (Channel Islands)",
                    "Bermuda",
                    "British Virgin Islands",
                    "Netherlands",
                ],
                "notes": (
                    "Glencore's corporate structure is famously opaque. Hundreds of "
                    "subsidiaries across tax havens. Swiss HQ in Zug canton provides "
                    "favorable corporate tax treatment. ICIJ Paradise Papers revealed "
                    "extensive offshore network."
                ),
                "confidence": "confirmed",
                "source": "ICIJ Paradise Papers, Glencore Annual Report (subsidiaries list)",
            },

            "controversies": [
                {
                    "issue": "DOJ $1.1B FCPA plea",
                    "detail": "See doj_plea_2022 section",
                    "confidence": "confirmed",
                },
                {
                    "issue": "DRC cobalt mining — child labor and environmental damage",
                    "detail": (
                        "Glencore's Katanga and Mutanda mines in DRC linked to artisanal "
                        "mining child labor in surrounding areas. DRC accounts for ~70% "
                        "of global cobalt supply. Amnesty International reports."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "issue": "Coal strategy — last man standing",
                    "detail": (
                        "Unlike BHP and Rio Tinto which divested coal, Glencore's strategy "
                        "is to 'responsibly run down' coal assets, extracting maximum value "
                        "while peers exit. Critics call it greenwashing; shareholders have "
                        "supported the approach."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "issue": "Marc Rich pardon scandal",
                    "detail": "See key_people > Marc Rich entry",
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "DOJ compliance monitor expires ~2025",
                    "implication": "Overhang removal when monitor period ends",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Viterra/Bunge deal — cash + Bunge shares",
                    "implication": "Glencore becomes major grain market player via Bunge stake",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Coal 'responsible rundown' generates massive cash flow",
                    "implication": "Coal cash funds copper/cobalt M&A — watch for targets",
                    "confidence": "derived",
                },
                {
                    "signal": "Dual trading+mining model = information edge",
                    "implication": "Glencore's physical market intelligence is unmatched",
                    "confidence": "inferred",
                },
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════
    # PRECIOUS METALS PRODUCERS
    # ══════════════════════════════════════════════════════════════════

    "precious_metals_producers": {

        # ── GOLD PRODUCERS ─────────────────────────────────────────

        "NEM": {
            "name": "Newmont Corporation",
            "ticker": "NEM",
            "exchange": "NYSE, ASX",
            "sector": "Gold Mining",
            "market_cap_usd": 52_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 18_600_000_000,
            "revenue_confidence": "confirmed",
            "employees": 31_600,
            "hq": "Denver, Colorado",

            "ceo": {
                "name": "Tom Palmer",
                "title": "President & CEO",
                "appointed": "2019-10-01",
                "confidence": "confirmed",
            },

            "key_facts": (
                "World's largest gold miner after 2023 acquisition of Newcrest Mining "
                "(Australia) for $17.8B. Now operates mines on 5 continents. Production "
                "target ~7M oz/year. Selling non-core assets post-Newcrest to simplify "
                "portfolio (divesting Telfer, Akyem, etc.)."
            ),

            "major_acquisitions": [
                {
                    "target": "Newcrest Mining",
                    "value_usd": 17_800_000_000,
                    "closed": "2023-11",
                    "strategic_rationale": "Created world's largest gold miner by production and reserves",
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "Gold price above $2,000/oz — NEM leverage to gold price",
                    "implication": "NEM is highest-cap gold equity; institutional gold proxy",
                    "confidence": "derived",
                },
                {
                    "signal": "Non-core asset sales post-Newcrest ($2B+ divestiture program)",
                    "implication": "Proceeds fund buybacks/dividends — watch for buyer identities",
                    "confidence": "confirmed",
                },
            ],
        },

        "GOLD": {
            "name": "Barrick Gold Corporation",
            "ticker": "GOLD",
            "exchange": "NYSE, TSX",
            "sector": "Gold Mining",
            "market_cap_usd": 32_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 12_900_000_000,
            "revenue_confidence": "confirmed",
            "employees": 22_000,
            "hq": "Toronto, Canada",

            "ceo": {
                "name": "Mark Bristow",
                "title": "President & CEO",
                "appointed": "2019-01-01",
                "background": (
                    "South African-born geologist. Former CEO of Randgold Resources. "
                    "Merged Randgold into Barrick in 2019 and took CEO role. Known for "
                    "discipline and Africa expertise."
                ),
                "confidence": "confirmed",
            },

            "key_assets": [
                {
                    "asset": "Nevada Gold Mines (JV with Newmont)",
                    "type": "gold",
                    "ownership_pct": 61.5,
                    "notes": "Largest gold mining complex in the world, ~3.5M oz/year combined",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Loulo-Gounkoto (Mali)",
                    "type": "gold",
                    "notes": "Key African asset — subject to Mali government resource nationalism",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Reko Diq (Pakistan)",
                    "type": "copper_gold",
                    "ownership_pct": 50,
                    "notes": "Major development project, resolved decades-long dispute with Pakistan",
                    "confidence": "confirmed",
                },
            ],

            "controversies": [
                {
                    "issue": "Tanzania dispute",
                    "detail": (
                        "Tanzania government under President Magufuli accused Barrick of "
                        "massive tax evasion. $190B tax bill threatened. Resolved via "
                        "partnership framework giving Tanzania 16% of Barrick subsidiary "
                        "and $300M payment."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "issue": "Papua New Guinea — Porgera mine violence",
                    "detail": (
                        "Porgera gold mine linked to extrajudicial killings and sexual "
                        "violence by mine security guards. Mine temporarily nationalized "
                        "by PNG government 2020, reopened 2023 under new terms."
                    ),
                    "confidence": "confirmed",
                    "source": "Human Rights Watch, Amnesty International",
                },
            ],
        },

        "AEM": {
            "name": "Agnico Eagle Mines Limited",
            "ticker": "AEM",
            "exchange": "NYSE, TSX",
            "sector": "Gold Mining",
            "market_cap_usd": 40_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 7_300_000_000,
            "revenue_confidence": "confirmed",
            "hq": "Toronto, Canada",

            "ceo": {
                "name": "Ammar Al-Joundi",
                "title": "President & CEO",
                "appointed": "2022-02-01",
                "confidence": "confirmed",
            },

            "key_facts": (
                "Third-largest gold producer globally. Premium valuation due to "
                "low-risk jurisdictions (Canada, Australia, Finland, Mexico). "
                "Merged with Kirkland Lake Gold in 2022. Known for operational "
                "excellence and consistent dividend."
            ),

            "trading_signals": [
                {
                    "signal": "Premium to NAV — 'safe haven' gold stock",
                    "implication": "AEM outperforms in risk-off environments; underperforms when gold lags",
                    "confidence": "derived",
                },
            ],
        },

        # ── SILVER PRODUCERS ───────────────────────────────────────

        "AG": {
            "name": "First Majestic Silver Corp.",
            "ticker": "AG",
            "exchange": "NYSE, TSX",
            "sector": "Silver Mining",
            "market_cap_usd": 3_500_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 660_000_000,
            "revenue_confidence": "confirmed",
            "hq": "Vancouver, Canada",

            "ceo": {
                "name": "Keith Neumeyer",
                "title": "President & CEO",
                "appointed": "2002 (founder-CEO)",
                "background": (
                    "Outspoken silver bull. Has publicly called for silver price "
                    "manipulation investigation. Advocates for physical silver investment. "
                    "Popular figure in precious metals community."
                ),
                "confidence": "confirmed",
            },

            "key_facts": (
                "Primary silver producer with mines in Mexico. Recently acquired "
                "Gatos Silver (Cerro Los Gatos mine). High-beta silver play — "
                "stock moves 2-3x silver price moves. Keith Neumeyer is one of "
                "few CEO silver advocates."
            ),
        },

        "PAAS": {
            "name": "Pan American Silver Corp.",
            "ticker": "PAAS",
            "exchange": "NYSE, TSX",
            "sector": "Silver / Gold Mining",
            "market_cap_usd": 8_500_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 2_800_000_000,
            "revenue_confidence": "confirmed",
            "hq": "Vancouver, Canada",

            "key_facts": (
                "World's second-largest primary silver producer. Operations across "
                "Latin America and Canada. Acquired Yamana Gold's assets in 2023 "
                "to add gold production, now ~50/50 silver/gold revenue mix."
            ),
        },

        # ── PLATINUM GROUP METALS ──────────────────────────────────

        "AMS_JSE": {
            "name": "Anglo American Platinum Limited (Amplats)",
            "ticker": "AMS (JSE)",
            "exchange": "JSE (Johannesburg)",
            "sector": "Platinum Group Metals (PGMs)",
            "market_cap_usd": 12_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 6_500_000_000,
            "revenue_confidence": "estimated",
            "hq": "Johannesburg, South Africa",

            "ownership": {
                "parent": "Anglo American plc (~79%)",
                "notes": (
                    "If BHP had succeeded in its Anglo American takeover bid (2024), "
                    "Amplats would have been demerged and separately listed, per BHP's "
                    "requirement to avoid South African complications."
                ),
                "confidence": "confirmed",
            },

            "key_facts": (
                "World's largest platinum producer. South Africa produces ~70% of "
                "global platinum. PGMs face structural headwind from EV adoption "
                "(catalytic converters are primary demand). Hydrogen fuel cell "
                "economy is the bull case for platinum."
            ),

            "trading_signals": [
                {
                    "signal": "EV adoption reduces catalytic converter demand",
                    "implication": "Structural bearish for platinum, unless hydrogen takes off",
                    "confidence": "derived",
                },
                {
                    "signal": "Anglo American restructuring — potential Amplats demerger",
                    "implication": "Standalone Amplats would trade at different multiple",
                    "confidence": "estimated",
                },
            ],
        },

        "SBSW": {
            "name": "Sibanye-Stillwater Limited",
            "ticker": "SBSW",
            "exchange": "NYSE, JSE",
            "sector": "PGMs / Gold / Lithium",
            "market_cap_usd": 3_500_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 5_200_000_000,
            "revenue_confidence": "estimated",
            "hq": "Johannesburg, South Africa",

            "ceo": {
                "name": "Neal Froneman",
                "title": "CEO",
                "appointed": "2013",
                "background": (
                    "Aggressive dealmaker. Built Sibanye from gold mine spinoff into "
                    "diversified miner via acquisitions (Stillwater PGMs in Montana, "
                    "Sandouville nickel in France, Keliber lithium in Finland). "
                    "Acquisitions have largely destroyed value — stock down >70% from peak."
                ),
                "confidence": "confirmed",
            },

            "key_facts": (
                "Owns Stillwater Mine in Montana — only US PGM mine. "
                "Major South African gold and PGM producer. Lithium diversification "
                "via Keliber (Finland) and exploration. Hit hard by falling PGM "
                "prices and rising costs in South Africa."
            ),

            "controversies": [
                {
                    "issue": "Value destruction through M&A",
                    "detail": (
                        "Stillwater acquisition ($2.2B, 2017) and subsequent investments "
                        "have underperformed. Sandouville nickel refinery (France) acquired "
                        "then plagued by operational issues. Stock down >70% from 2021 peak."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "issue": "South African labor unrest",
                    "detail": "Frequent strikes at gold operations; deep-level mining safety issues",
                    "confidence": "confirmed",
                },
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════
    # CENTRAL BANK GOLD RESERVES
    # ══════════════════════════════════════════════════════════════════

    "central_bank_gold_reserves": {
        "meta": {
            "description": (
                "Central banks have been net gold buyers since 2010, accelerating after "
                "2022 Russia sanctions froze $300B in reserves. De-dollarization and "
                "sanctions risk are driving record gold accumulation."
            ),
            "data_source": "World Gold Council, IMF IFS",
            "confidence": "confirmed",
        },

        "top_holders_tonnes": {
            "United States": {"tonnes": 8133, "pct_of_reserves": 69, "confidence": "confirmed"},
            "Germany": {"tonnes": 3352, "pct_of_reserves": 68, "confidence": "confirmed"},
            "Italy": {"tonnes": 2452, "pct_of_reserves": 65, "confidence": "confirmed"},
            "France": {"tonnes": 2437, "pct_of_reserves": 66, "confidence": "confirmed"},
            "Russia": {"tonnes": 2333, "pct_of_reserves": 26, "confidence": "confirmed"},
            "China": {"tonnes": 2262, "pct_of_reserves": 4.9, "confidence": "estimated",
                       "notes": "Official figure widely believed to understate true holdings. "
                                "PBOC has added 300+ tonnes since 2022. Actual holdings may be "
                                "3,000-5,000 tonnes via SAFE and CIC."},
            "Switzerland": {"tonnes": 1040, "pct_of_reserves": 8, "confidence": "confirmed"},
            "Japan": {"tonnes": 846, "pct_of_reserves": 4.3, "confidence": "confirmed"},
            "India": {"tonnes": 854, "pct_of_reserves": 9.3, "confidence": "confirmed"},
            "Netherlands": {"tonnes": 612, "pct_of_reserves": 56, "confidence": "confirmed"},
        },

        "biggest_recent_buyers": [
            {
                "country": "China (PBOC)",
                "2022_2024_net_tonnes": 316,
                "notes": (
                    "Officially reported 316 tonnes added 2022-2024. True accumulation "
                    "likely much higher via undisclosed channels (SAFE, CIC). Accelerated "
                    "buying after Russia sanctions demonstrated reserves can be frozen."
                ),
                "confidence": "estimated",
                "motivation": "De-dollarization, sanctions insurance, reserve diversification",
            },
            {
                "country": "Poland (NBP)",
                "2022_2024_net_tonnes": 230,
                "notes": (
                    "Largest European gold buyer. NBP President Adam Glapiński has "
                    "explicitly stated goal of reaching 20% of reserves in gold. "
                    "Poland views gold as insurance against Russia/Ukraine spillover."
                ),
                "confidence": "confirmed",
                "source": "NBP official statements, World Gold Council",
                "motivation": "NATO frontier security, reserves diversification",
            },
            {
                "country": "India (RBI)",
                "2022_2024_net_tonnes": 120,
                "notes": "Steady accumulation. RBI repatriated gold from Bank of England.",
                "confidence": "confirmed",
                "motivation": "Reserve diversification, cultural affinity for gold",
            },
            {
                "country": "Turkey (CBRT)",
                "2022_2024_net_tonnes": 100,
                "notes": (
                    "Turkey has been both buyer and seller, using gold reserves to "
                    "stabilize the lira during currency crises. Net buyer overall."
                ),
                "confidence": "confirmed",
                "motivation": "Currency defense, Erdogan gold affinity, sanctions hedging",
            },
            {
                "country": "Singapore (MAS)",
                "2022_2024_net_tonnes": 75,
                "notes": "Quiet but consistent accumulation",
                "confidence": "confirmed",
            },
            {
                "country": "Czech Republic (CNB)",
                "2022_2024_net_tonnes": 55,
                "notes": "Significant proportional increase from low base",
                "confidence": "confirmed",
            },
        ],

        "trading_signals": [
            {
                "signal": "Central bank buying at record pace (1,000+ tonnes/year since 2022)",
                "implication": "Structural floor under gold prices; de-dollarization accelerating",
                "confidence": "confirmed",
            },
            {
                "signal": "China official figures likely understate true holdings by 50-100%",
                "implication": "True PBOC holdings reveal China's strategic intent re: USD system",
                "confidence": "estimated",
            },
            {
                "signal": "Russia sanctions froze $300B — catalyst for reserve diversification",
                "implication": "Non-aligned nations accelerating gold + non-USD reserve shifts",
                "confidence": "confirmed",
            },
        ],
    },

    # ══════════════════════════════════════════════════════════════════
    # AGRICULTURE TECHNOLOGY & CHEMICAL MONOPOLIES
    # ══════════════════════════════════════════════════════════════════

    "agtech_monopolies": {

        # ── DEERE & CO ─────────────────────────────────────────────
        "DE": {
            "name": "Deere & Company",
            "ticker": "DE",
            "exchange": "NYSE",
            "sector": "Agricultural Equipment / Precision Agriculture",
            "market_cap_usd": 110_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_fy2024_usd": 51_700_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024 (Oct year-end)",
            "net_income_fy2024_usd": 7_100_000_000,
            "net_income_confidence": "confirmed",
            "employees": 83_000,
            "hq": "Moline, Illinois",
            "founded": 1837,

            "ceo": {
                "name": "John C. May",
                "title": "Chairman & CEO",
                "appointed": "2019-11-04",
                "total_compensation_fy2024_usd": 26_700_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2024",
                "confidence": "confirmed",
            },

            "precision_ag_monopoly": {
                "market_position": (
                    "Deere controls an estimated 60%+ of the US large agricultural "
                    "equipment market and is building a precision agriculture technology "
                    "moat via GPS guidance, autonomous driving, sensor-based spraying, "
                    "and AI crop analysis. Farmers who buy Deere equipment are increasingly "
                    "locked into Deere's data platform and software subscription ecosystem."
                ),
                "data_lock_in": (
                    "Deere's John Deere Operations Center collects granular field-level "
                    "data (soil, yield, weather, application rates) from every connected "
                    "machine. Farmers have limited ability to export or use this data "
                    "outside Deere's ecosystem. This data monopoly is potentially more "
                    "valuable than the equipment business."
                ),
                "autonomous_farming": (
                    "Deere acquired Blue River Technology ($305M, 2017) for see-and-spray "
                    "AI and Bear Flag Robotics ($250M, 2021) for autonomous tractors. "
                    "Fully autonomous 8R tractor launched commercially 2022."
                ),
                "confidence": "derived",
            },

            "right_to_repair": {
                "issue": (
                    "Deere has been the primary target of the right-to-repair movement. "
                    "Tractors require Deere-authorized software tools for most repairs, "
                    "forcing farmers to use expensive authorized dealers. Farmers have "
                    "resorted to Ukrainian firmware hacks to bypass software locks."
                ),
                "legal_developments": [
                    "FTC launched right-to-repair enforcement initiative 2021",
                    "Colorado passed agricultural right-to-repair law 2023",
                    "Deere signed voluntary MOU with American Farm Bureau 2023 to provide repair tools",
                    "Critics say MOU is insufficient — no actual software unlocking",
                    "Multiple state legislatures pursuing mandatory repair access bills",
                ],
                "financial_impact": (
                    "Deere's parts and service segment is a high-margin profit center "
                    "(~70% gross margin vs ~35% for equipment). Right-to-repair "
                    "legislation could compress these margins by $1-2B annually."
                ),
                "confidence": "confirmed",
                "source": "FTC reports, state legislation, Bloomberg, PIRG reports",
            },

            "lobbying": {
                "annual_spend_usd": 5_400_000,
                "year": 2023,
                "key_issues": [
                    "Right-to-repair legislation (opposing mandatory provisions)",
                    "Farm Bill provisions",
                    "Trade policy (equipment tariffs)",
                    "Autonomous vehicle regulation",
                    "Data privacy (opposing farmer data portability requirements)",
                ],
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "pac_contributions": {
                "pac_name": "Deere & Company PAC",
                "total_2024_cycle_usd": 1_200_000,
                "split_pct": {"republican": 60, "democrat": 40},
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "trading_signals": [
                {
                    "signal": "Ag equipment cycle peaked — farmer income declining from 2022 highs",
                    "implication": "DE cyclical downturn, but precision ag subscription revenue growing",
                    "confidence": "derived",
                },
                {
                    "signal": "Right-to-repair legislation risk to service margins",
                    "implication": "Watch Colorado and other state bills; FTC enforcement actions",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Data monopoly in precision ag = future recurring revenue moat",
                    "implication": "Transition from equipment sales to SaaS-like recurring revenue",
                    "confidence": "inferred",
                },
            ],
        },

        # ── CORTEVA ────────────────────────────────────────────────
        "CTVA": {
            "name": "Corteva, Inc.",
            "ticker": "CTVA",
            "exchange": "NYSE",
            "sector": "Agricultural Chemicals / Seeds",
            "market_cap_usd": 38_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 17_100_000_000,
            "revenue_confidence": "confirmed",
            "employees": 21_000,
            "hq": "Indianapolis, Indiana",
            "founded": "2019 (spinoff from DowDuPont)",

            "ceo": {
                "name": "Chuck Magro",
                "title": "CEO",
                "appointed": "2022-01-01",
                "background": "Former CEO of Nutrien; recruited to lead Corteva",
                "confidence": "confirmed",
            },

            "corporate_history": {
                "lineage": (
                    "Corteva is the agricultural sciences spinoff from the DowDuPont "
                    "merger and subsequent three-way split. Contains legacy Pioneer "
                    "Hi-Bred (seeds, acquired by DuPont for $7.7B in 1999) and DuPont "
                    "Crop Protection (herbicides, insecticides, fungicides). The Dow/DuPont "
                    "merger ($130B, 2017) was engineered by activist Nelson Peltz and "
                    "designed to separate into Dow (materials), DuPont (specialty), "
                    "and Corteva (agriculture)."
                ),
                "confidence": "confirmed",
            },

            "market_position": {
                "seeds": "Second-largest seed company globally (behind Bayer/Monsanto)",
                "crop_protection": "Top 5 crop protection globally",
                "key_products": [
                    "Pioneer brand corn and soybean seed",
                    "Enlist herbicide system (alternative to Roundup/dicamba)",
                    "Rinskor herbicide (rice)",
                ],
                "confidence": "confirmed",
            },

            "trading_signals": [
                {
                    "signal": "Beneficiary of Bayer/Monsanto Roundup litigation disaster",
                    "implication": "Corteva's Enlist system gaining share as farmers flee dicamba/Roundup",
                    "confidence": "derived",
                },
                {
                    "signal": "Seed pricing power — high barriers to entry",
                    "implication": "Oligopoly with Bayer/BASF/Syngenta provides pricing cushion",
                    "confidence": "derived",
                },
            ],
        },

        # ── NUTRIEN ────────────────────────────────────────────────
        "NTR": {
            "name": "Nutrien Ltd.",
            "ticker": "NTR",
            "exchange": "NYSE, TSX",
            "sector": "Fertilizer / Potash / Nitrogen / Phosphate",
            "market_cap_usd": 24_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 24_300_000_000,
            "revenue_confidence": "confirmed",
            "employees": 23_500,
            "hq": "Saskatoon, Saskatchewan, Canada",
            "founded": "2018 (merger of PotashCorp + Agrium)",

            "ceo": {
                "name": "Ken Seitz",
                "title": "President & CEO",
                "appointed": "2023-08-01",
                "confidence": "confirmed",
            },

            "market_position": {
                "potash": (
                    "World's largest potash producer. Controls ~20% of global potash "
                    "capacity through Saskatchewan mines. Potash is an oligopoly: "
                    "Nutrien, Belaruskali (Belarus, sanctioned), Uralkali (Russia, "
                    "sanctioned/restricted), and Mosaic (US) control >70% of global supply."
                ),
                "nitrogen": "Major nitrogen producer (Trinidad, US, Canada plants)",
                "phosphate": "Major phosphate producer (US operations)",
                "retail": (
                    "World's largest crop input retailer — ~2,000 retail locations "
                    "across US, Canada, South America, Australia. Direct farmer relationship."
                ),
                "confidence": "confirmed",
            },

            "sanctions_windfall": {
                "detail": (
                    "Russia/Belarus sanctions post-2022 Ukraine invasion disrupted "
                    "~40% of global potash supply (Uralkali + Belaruskali). Nutrien's "
                    "potash profits surged from ~$2B to ~$7B in 2022. Potash prices "
                    "have since normalized but remain elevated vs pre-war levels."
                ),
                "confidence": "confirmed",
            },

            "controversies": [
                {
                    "issue": "Potash price gouging allegations",
                    "detail": (
                        "Fertilizer price spikes in 2022 led to accusations that Nutrien "
                        "and other producers profiteered during food crisis. US Senate "
                        "Agriculture Committee hearings. Nutrien defended pricing as "
                        "market-driven."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "issue": "Saskatchewan mining environmental concerns",
                    "detail": "Potash brine ponds and underground mining create subsidence risk",
                    "confidence": "confirmed",
                },
            ],

            "lobbying": {
                "annual_spend_usd": 1_600_000,
                "year": 2023,
                "key_issues": [
                    "Fertilizer tariff policy",
                    "Farm Bill nutrient management",
                    "Sanctions enforcement (Belarus/Russia potash)",
                    "Carbon capture tax credits",
                ],
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "trading_signals": [
                {
                    "signal": "Potash oligopoly — sanctions keep Russia/Belarus supply constrained",
                    "implication": "Nutrien benefits as long as sanctions persist on Belaruskali/Uralkali",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Potash prices normalizing from 2022 spike",
                    "implication": "Revenue declining from peak — watch WASDE and planting intentions",
                    "confidence": "confirmed",
                },
                {
                    "signal": "BHP Jansen potash mine starting ~2026",
                    "implication": "New supply could pressure potash prices long-term",
                    "confidence": "estimated",
                },
            ],
        },

        # ── BAYER / MONSANTO ───────────────────────────────────────
        "BAYN": {
            "name": "Bayer AG",
            "ticker": "BAYN (XETRA) / BAYRY (OTC)",
            "exchange": "XETRA (Frankfurt), OTC (US)",
            "sector": "Pharma + Crop Science (Monsanto)",
            "market_cap_usd": 25_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 47_600_000_000,
            "revenue_confidence": "confirmed",
            "employees": 100_000,
            "hq": "Leverkusen, Germany",
            "founded": 1863,

            "ceo": {
                "name": "Bill Anderson",
                "title": "CEO",
                "appointed": "2023-06-01",
                "background": (
                    "American executive, former head of Roche Pharmaceuticals. "
                    "Brought in to turnaround Bayer after Monsanto acquisition disaster. "
                    "Replaced Werner Baumann who executed the Monsanto deal."
                ),
                "confidence": "confirmed",
            },

            "monsanto_acquisition": {
                "closed": "2018-06-07",
                "value_usd": 63_000_000_000,
                "detail": (
                    "Widely considered the worst acquisition in corporate history. "
                    "Bayer's market cap was ~$60B at purchase; by 2024 it had fallen "
                    "below $25B — meaning the Monsanto acquisition destroyed over $35B "
                    "in shareholder value. The Roundup litigation alone has consumed "
                    "$10B+ in settlements with ~60,000 cases still pending."
                ),
                "confidence": "confirmed",
            },

            "roundup_litigation": {
                "product": "Roundup (glyphosate herbicide)",
                "original_manufacturer": "Monsanto",
                "total_claims": 165_000,
                "settled_claims": 105_000,
                "total_settlement_cost_usd": 10_900_000_000,
                "remaining_claims": 60_000,
                "estimated_remaining_liability_usd": 5_000_000_000,
                "key_verdicts": [
                    {
                        "case": "Johnson v. Monsanto (2018)",
                        "plaintiff": "Dewayne Johnson, school groundskeeper",
                        "verdict_usd": 289_000_000,
                        "reduced_to_usd": 78_500_000,
                        "significance": "First Roundup cancer verdict; triggered flood of lawsuits",
                        "confidence": "confirmed",
                    },
                    {
                        "case": "Pilliod v. Monsanto (2019)",
                        "verdict_usd": 2_055_000_000,
                        "reduced_to_usd": 87_000_000,
                        "confidence": "confirmed",
                    },
                    {
                        "case": "Hardeman v. Monsanto (2019)",
                        "verdict_usd": 80_000_000,
                        "confidence": "confirmed",
                    },
                ],
                "regulatory_status": (
                    "EPA reaffirmed that glyphosate is 'not likely to be carcinogenic to humans' "
                    "but IARC (WHO) classified it as 'probably carcinogenic' in 2015. "
                    "This conflict underpins ongoing litigation."
                ),
                "confidence": "confirmed",
                "source": "Court filings, Bayer Annual Report, Reuters",
            },

            "crop_science_division": {
                "revenue_2024_usd": 20_200_000_000,
                "market_position": (
                    "World's largest seed and crop protection company post-Monsanto. "
                    "Controls dominant share of US corn and soybean seed (Dekalb, Asgrow brands). "
                    "Roundup Ready (glyphosate-tolerant) trait in ~90% of US soybeans."
                ),
                "crispr_crops": {
                    "detail": (
                        "Bayer holds exclusive license from Broad Institute for agricultural "
                        "use of CRISPR-Cas9 gene editing. Developing non-transgenic gene-edited "
                        "crops (shorter corn, drought-resistant wheat) that may face less "
                        "regulatory burden than traditional GMOs."
                    ),
                    "significance": (
                        "CRISPR crops could be the next revolution in agriculture — and Bayer "
                        "controls the foundational IP. Short-stature corn (3 feet shorter) "
                        "resists wind damage and allows denser planting."
                    ),
                    "confidence": "confirmed",
                    "source": "Bayer Crop Science presentations, Broad Institute license",
                },
                "confidence": "confirmed",
            },

            "controversies": [
                {
                    "issue": "Roundup/glyphosate litigation ($10B+)",
                    "detail": "See roundup_litigation section",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Monsanto acquisition value destruction",
                    "detail": "See monsanto_acquisition section",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Monsanto 'Fusion Center' surveillance",
                    "detail": (
                        "Monsanto maintained secret lists of journalists, politicians, and "
                        "activists critical of Roundup/GMOs. Le Monde revealed the list in "
                        "2019. French prosecutors opened criminal investigation."
                    ),
                    "confidence": "confirmed",
                    "source": "Le Monde, French judicial investigation",
                },
                {
                    "issue": "PCB environmental liability (Monsanto legacy)",
                    "detail": (
                        "Monsanto was a major producer of PCBs (banned 1979). Ongoing "
                        "environmental cleanup litigation in multiple US cities and states. "
                        "Bayer inherited this liability."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "Roundup litigation tail risk — 60,000 cases still pending",
                    "implication": "Each adverse verdict resets settlement expectations upward",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Break-up speculation — Bayer could split pharma from crop science",
                    "implication": "Sum-of-parts analysis suggests break-up value >50% premium",
                    "confidence": "estimated",
                },
                {
                    "signal": "CRISPR crop science — long-term option value",
                    "implication": "Bayer's CRISPR license is undervalued in current beaten-down stock",
                    "confidence": "inferred",
                },
                {
                    "signal": "New CEO Bill Anderson — turnaround catalyst",
                    "implication": "Restructuring, potential divestitures, litigation resolution push",
                    "confidence": "derived",
                },
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════
    # FOOD SECURITY — GEOPOLITICAL RISK SIGNALS
    # ══════════════════════════════════════════════════════════════════

    "food_security": {
        "meta": {
            "description": (
                "Food security is a geopolitical weapon and systemic risk factor. "
                "Grain supply disruptions, weather events, and water scarcity create "
                "cascading effects through commodity markets, inflation, and social stability."
            ),
            "confidence": "confirmed",
        },

        "fao_food_price_index": {
            "description": (
                "UN FAO Food Price Index tracks monthly changes in international "
                "prices of a basket of food commodities (cereals, oils, dairy, meat, sugar). "
                "The index spiked to all-time highs in March 2022 after Russia's Ukraine "
                "invasion, exceeding the 2011 Arab Spring food price spike."
            ),
            "2022_peak": 159.7,
            "2024_level": 120,
            "2011_arab_spring_peak": 131.9,
            "baseline_2014_2016": 100,
            "notes": (
                "2022 spike directly linked to Russia-Ukraine conflict disrupting "
                "Black Sea grain exports (~30% of global wheat exports). The 2011 "
                "spike is widely credited as a trigger for the Arab Spring revolutions."
            ),
            "trading_signal": (
                "FAO index >130 historically correlates with political instability in "
                "food-importing nations (Middle East, North Africa). Monitor for "
                "social unrest, regime change risk, and EM currency pressure."
            ),
            "confidence": "confirmed",
            "source": "UN FAO monthly reports",
        },

        "ukraine_russia_grain_corridor": {
            "background": (
                "Russia and Ukraine together account for ~30% of global wheat exports, "
                "~20% of corn exports, and ~80% of sunflower oil exports. Russia's "
                "February 2022 invasion and Black Sea blockade triggered the sharpest "
                "food price spike since the 2007-2008 crisis."
            ),
            "black_sea_grain_initiative": {
                "brokered_by": "Turkey and United Nations",
                "started": "2022-07",
                "ended": "2023-07 (Russia withdrew)",
                "total_exported_tonnes": 33_000_000,
                "notes": (
                    "After Russia withdrew, Ukraine established its own corridor via "
                    "western Black Sea routes hugging NATO-member coastlines. Ukrainian "
                    "grain exports partially recovered but remain below pre-war levels."
                ),
                "confidence": "confirmed",
            },
            "russia_as_food_weapon": (
                "Russia has weaponized grain exports as geopolitical leverage. "
                "Putin has offered discounted grain to African nations in exchange "
                "for political alignment. Russia is now world's largest wheat exporter "
                "and uses food diplomacy extensively."
            ),
            "trading_signals": [
                {
                    "signal": "Black Sea shipping risk — insurance premiums elevated",
                    "implication": "Grain shipping cost premium persists; benefits US/Brazil/Argentina exporters",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Russia capturing Ukraine export share",
                    "implication": "Russian wheat exports at record levels; monitor USDA FAS data",
                    "confidence": "confirmed",
                },
            ],
            "confidence": "confirmed",
            "source": "UN, USDA, Black Sea Grain Initiative reports",
        },

        "el_nino_la_nina_impact": {
            "description": (
                "ENSO (El Niño-Southern Oscillation) is the most impactful weather "
                "pattern for global agriculture. El Niño and La Niña create predictable "
                "crop impacts across hemispheres."
            ),
            "el_nino_effects": {
                "positive": [
                    "US Midwest — generally favorable for corn and soybeans",
                    "Argentina — typically above-average rainfall, good for crops",
                ],
                "negative": [
                    "Australia — drought conditions, wheat production falls",
                    "India — weak monsoon, rice and wheat production drops",
                    "Southeast Asia — drought, palm oil production declines",
                    "Southern Africa — drought, maize production falls",
                ],
                "confidence": "confirmed",
            },
            "la_nina_effects": {
                "positive": [
                    "Australia — above-average rainfall, strong wheat crop",
                    "India — strong monsoon, good rice production",
                    "Southeast Asia — good palm oil conditions",
                ],
                "negative": [
                    "US Southern Plains — drought (wheat, cotton)",
                    "Argentina/Southern Brazil — drought risk for soy, corn",
                    "East Africa — drought and food insecurity",
                ],
                "confidence": "confirmed",
            },
            "current_status_2025": {
                "phase": "Transitioning from El Niño to neutral/La Niña",
                "notes": "2024-2025 El Niño was moderate; La Niña watch for 2025-2026",
                "confidence": "estimated",
                "source": "NOAA CPC, Australian BOM",
            },
            "trading_signals": [
                {
                    "signal": "La Niña developing — historical drought risk for Argentina/Brazil",
                    "implication": "Long soybeans/corn if La Niña confirmed; short Australian wheat",
                    "confidence": "estimated",
                },
                {
                    "signal": "ENSO transitions are 6-12 month lead indicators for crop yields",
                    "implication": "Monitor NOAA CPC weekly ENSO updates for positioning",
                    "confidence": "derived",
                },
            ],
        },

        "water_scarcity": {
            "description": (
                "Agriculture consumes ~70% of global freshwater. Water scarcity is the "
                "ultimate constraint on food production and one of the most underpriced "
                "risks in commodity markets."
            ),
            "nestle_water_rights_controversy": {
                "detail": (
                    "Nestlé (and its former water subsidiary, now BlueTriton Brands after "
                    "2021 sale) has faced decades of controversy over extracting water from "
                    "public sources at minimal cost and selling it as bottled water. Key "
                    "flashpoints include: Flint, Michigan (extracting water during water crisis), "
                    "California (extracting on expired permits during drought), Ontario Canada "
                    "(outbidding municipalities for water permits). Former Nestlé CEO Peter "
                    "Brabeck-Letmathe's 2005 statement that water 'should have a market value' "
                    "became a lightning rod."
                ),
                "blueTriton_sale": (
                    "Nestlé sold its North American water brands (Poland Spring, Deer Park, etc.) "
                    "to One Rock Capital for $4.3B in 2021, rebranded as BlueTriton Brands. "
                    "Controversies continue under new ownership."
                ),
                "confidence": "confirmed",
                "source": "Reuters, NYT, Canadian Broadcasting, Nestlé filings",
            },
            "ogallala_aquifer_depletion": {
                "detail": (
                    "The Ogallala Aquifer underlies 8 US states and irrigates ~30% of US "
                    "cropland. It is being depleted faster than natural recharge — parts of "
                    "Kansas, Texas, and Oklahoma could be exhausted within 25 years. This "
                    "threatens the US grain belt's long-term production capacity."
                ),
                "confidence": "confirmed",
                "source": "USGS, Kansas Geological Survey",
            },
            "global_hotspots": [
                {
                    "region": "India",
                    "issue": "Groundwater depletion in Punjab/Haryana threatens wheat/rice production",
                    "confidence": "confirmed",
                },
                {
                    "region": "Middle East/North Africa",
                    "issue": "Critical water stress; MENA nations import >50% of food",
                    "confidence": "confirmed",
                },
                {
                    "region": "Central Valley, California",
                    "issue": "Aquifer overdraft, land subsidence, conflict with urban water demand",
                    "confidence": "confirmed",
                },
                {
                    "region": "Colorado River Basin",
                    "issue": (
                        "Lake Mead/Powell at historically low levels. 7 states + Mexico "
                        "compete for declining water. Agriculture takes ~70% of allocation."
                    ),
                    "confidence": "confirmed",
                },
            ],
            "trading_signals": [
                {
                    "signal": "Water scarcity is the ultimate constraint on ag production growth",
                    "implication": "Long-term bullish for crop prices; bearish for water-intensive agriculture",
                    "confidence": "derived",
                },
                {
                    "signal": "Water rights companies (Xylem, Evoqua, Veolia) as plays on scarcity",
                    "implication": "Water infrastructure is a secular growth theme",
                    "confidence": "inferred",
                },
            ],
        },

        "risk_signals": [
            {
                "signal": "FAO Food Price Index elevated above 2014-2016 baseline",
                "severity": "medium",
                "affected_assets": ["grain futures", "fertilizer stocks", "EM currencies"],
                "confidence": "confirmed",
            },
            {
                "signal": "Black Sea grain corridor disrupted — Russia using food as weapon",
                "severity": "high",
                "affected_assets": ["CBOT wheat", "corn", "sunflower oil", "shipping rates"],
                "confidence": "confirmed",
            },
            {
                "signal": "La Niña watch — drought risk for South American crops",
                "severity": "medium",
                "affected_assets": ["soybeans", "corn", "BG", "ADM", "NTR"],
                "confidence": "estimated",
            },
            {
                "signal": "Ogallala aquifer depletion — US grain belt structural risk",
                "severity": "high",
                "timeframe": "10-25 years",
                "affected_assets": ["US wheat/corn futures", "farmland REITs", "DE"],
                "confidence": "confirmed",
            },
            {
                "signal": "Global fertilizer supply constrained by Russia/Belarus sanctions",
                "severity": "medium",
                "affected_assets": ["NTR", "MOS", "CF", "potash/nitrogen/phosphate futures"],
                "confidence": "confirmed",
            },
            {
                "signal": "China stockpiling grain — strategic reserves opaque",
                "severity": "medium",
                "affected_assets": ["CBOT wheat/corn/soybeans", "COFCO partners"],
                "confidence": "estimated",
            },
        ],
    },

    # ══════════════════════════════════════════════════════════════════
    # CROSS-REFERENCES & INTERCONNECTIONS
    # ══════════════════════════════════════════════════════════════════

    "cross_references": {
        "abcd_oligopoly": {
            "description": (
                "The ABCD traders (ADM, Bunge, Cargill, Louis Dreyfus) plus COFCO "
                "control an estimated 70-90% of global grain trade. This oligopoly "
                "provides information asymmetry — they see physical flows before "
                "financial markets do."
            ),
            "members": ["ADM", "BG", "CARGILL", "LOUIS_DREYFUS", "COFCO"],
            "combined_revenue_usd": 466_000_000_000,
            "confidence": "estimated",
        },
        "mining_copper_thesis": {
            "description": (
                "BHP, Rio Tinto, Freeport-McMoRan, and Glencore are all pivoting "
                "toward copper — the critical metal for electrification. Competition "
                "for copper assets is intensifying (BHP's Anglo American bid, etc.)."
            ),
            "key_players": ["BHP", "RIO", "FCX", "GLEN"],
            "confidence": "confirmed",
        },
        "glencore_nexus": {
            "description": (
                "Glencore sits at the intersection of mining, commodity trading, and "
                "now agriculture (via Bunge/Viterra). It has the broadest commodity "
                "market intelligence of any single entity."
            ),
            "connections": ["GLEN → BG (18% stake post-Viterra)", "GLEN ↔ COFCO (coal/metals trading)"],
            "confidence": "confirmed",
        },
        "seed_chemical_oligopoly": {
            "description": (
                "Global seed and crop chemical market is controlled by 4 companies: "
                "Bayer (Monsanto), Corteva (DowDuPont), Syngenta (ChemChina/Sinochem), "
                "and BASF. They control >60% of global seed sales and >70% of crop "
                "chemical sales."
            ),
            "members": ["BAYN", "CTVA", "Syngenta (private - ChemChina)", "BASF"],
            "confidence": "confirmed",
        },
        "energy_food_nexus": {
            "description": (
                "Energy and food prices are deeply interlinked. Natural gas is the "
                "primary input for nitrogen fertilizer (Haber-Bosch process). Oil prices "
                "drive farm equipment and transportation costs. Biofuel mandates (ethanol, "
                "biodiesel) create direct food-fuel competition for corn and soybeans."
            ),
            "confidence": "confirmed",
        },
    },
}
