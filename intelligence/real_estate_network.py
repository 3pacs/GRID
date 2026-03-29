"""
GRID Intelligence -- Global Real Estate & REIT Power Network.

Maps the global real estate power structure: US REITs (industrial, tower,
data center, retail, office), global property empires (UAE royals, HK tycoons,
Singapore/Japan REITs, Chinese developers), private real estate titans
(Blackstone, Brookfield, Starwood, Irvine Co), and the commercial real estate
crisis (office vacancies, regional bank CRE exposure, CMBS delinquency, WFH
impact).

Real estate is the largest asset class on Earth (~$380T global value).
US REITs alone hold ~$4T in assets.  The CRE crisis post-COVID is the
single largest systemic risk vector for regional banks, CMBS markets,
and municipal tax bases.  Private capital (Blackstone, Brookfield) has
replaced public markets as the marginal buyer, creating liquidity
transformation risk when retail investors in non-traded REITs demand
redemptions.

Confidence labels per GRID convention:
    confirmed  -- public filings, annual reports, regulatory data
    derived    -- calculated from multiple confirmed sources
    estimated  -- credible third-party estimate (Green Street, MSCI, CoStar)
    rumored    -- media reports, unnamed sources
    inferred   -- pattern-based deduction without direct evidence

Key entry points:
    get_real_estate_network()           -- full network dict
    get_reit(key)                       -- single REIT dossier
    get_private_re(key)                 -- private RE firm dossier
    get_global_empire(key)              -- global property empire dossier
    get_cre_crisis_summary()            -- commercial RE crisis dashboard
    get_office_vacancy_by_city()        -- vacancy rates by metro
    get_regional_bank_cre_exposure()    -- banks most exposed to CRE
    get_political_connections_all()     -- all RE lobbying + political links
    get_offshore_structures_all()       -- all offshore/opaque structures

Sources:
    - SEC 10-K, DEF 14A, Form 4, 13F filings (all US REITs)
    - NAREIT T-Tracker quarterly data
    - Green Street Advisors CPPI + analytics
    - CoStar / CBRE vacancy and rent data
    - MSCI Real Capital Analytics
    - Trepp CMBS delinquency reports
    - Federal Reserve Senior Loan Officer Survey (SLOOS)
    - FDIC Call Reports (bank CRE concentration)
    - OpenSecrets lobbying and PAC data
    - ICIJ Panama Papers / Pandora Papers
    - Bloomberg, Financial Times, Reuters, WSJ reporting
    - Hong Kong Stock Exchange annual reports
    - Singapore Exchange REIT disclosures
    - Japan REIT (J-REIT) Association data
"""

from __future__ import annotations

from typing import Any


def get_real_estate_network() -> dict[str, Any]:
    """Return the full real estate power network."""
    return REAL_ESTATE_NETWORK


def get_reit(key: str) -> dict[str, Any] | None:
    """Return dossier for a single US REIT by key (e.g. 'pld', 'amt')."""
    return REAL_ESTATE_NETWORK.get("us_reits", {}).get(key)


def get_private_re(key: str) -> dict[str, Any] | None:
    """Return dossier for a private RE firm by key (e.g. 'blackstone_re')."""
    return REAL_ESTATE_NETWORK.get("private_real_estate", {}).get(key)


def get_global_empire(key: str) -> dict[str, Any] | None:
    """Return dossier for a global property empire by key."""
    for section in ("uae_royal_property", "chinese_developers", "hk_tycoons",
                     "singapore_reits", "japanese_reits"):
        entity = REAL_ESTATE_NETWORK.get(section, {}).get(key)
        if entity:
            return entity
    return None


def get_cre_crisis_summary() -> dict[str, Any]:
    """Return commercial real estate crisis dashboard."""
    return REAL_ESTATE_NETWORK.get("cre_crisis", {})


def get_office_vacancy_by_city() -> dict[str, Any]:
    """Return office vacancy rates by major metro."""
    return REAL_ESTATE_NETWORK.get("cre_crisis", {}).get("office_vacancy_by_city", {})


def get_regional_bank_cre_exposure() -> list[dict[str, Any]]:
    """Return regional banks most exposed to CRE."""
    return REAL_ESTATE_NETWORK.get("cre_crisis", {}).get("regional_bank_cre_exposure", [])


def get_political_connections_all() -> list[dict[str, Any]]:
    """Extract all political connections across all RE entities."""
    connections = []
    for section in ("us_reits", "private_real_estate"):
        for key, entity in REAL_ESTATE_NETWORK.get(section, {}).items():
            pol = entity.get("political_connections")
            if pol:
                entry = dict(pol)
                entry["entity_key"] = key
                entry["entity_name"] = entity.get("name", key)
                connections.append(entry)
    return connections


def get_offshore_structures_all() -> list[dict[str, Any]]:
    """Extract all offshore / opaque structures across all entities."""
    structures = []
    for section in ("us_reits", "private_real_estate", "uae_royal_property",
                     "hk_tycoons"):
        for key, entity in REAL_ESTATE_NETWORK.get(section, {}).items():
            offshore = entity.get("offshore_structures")
            if offshore:
                for s in (offshore if isinstance(offshore, list) else [offshore]):
                    entry = dict(s) if isinstance(s, dict) else {"detail": s}
                    entry["entity_key"] = key
                    entry["entity_name"] = entity.get("name", key)
                    structures.append(entry)
    return structures


# ==============================================================================
# MASTER DATA STRUCTURE
# ==============================================================================

REAL_ESTATE_NETWORK: dict[str, Any] = {
    "metadata": {
        "report_type": "global_real_estate_power_network",
        "version": "1.0.0",
        "generated": "2026-03-28",
        "confidence_schema": ["confirmed", "derived", "estimated", "rumored", "inferred"],
        "sources": [
            "SEC EDGAR (10-K, DEF 14A, Form 4, 13F)",
            "NAREIT T-Tracker",
            "Green Street Advisors",
            "CoStar / CBRE Research",
            "MSCI Real Capital Analytics",
            "Trepp CMBS Research",
            "Federal Reserve SLOOS",
            "FDIC Call Reports",
            "OpenSecrets.org",
            "ICIJ Panama Papers / Pandora Papers",
            "Bloomberg, FT, Reuters, WSJ",
            "HKEX Annual Reports",
            "SGX REIT Disclosures",
            "J-REIT Association",
        ],
        "data_vintage": "public information through early 2026",
        "update_frequency": "quarterly after earnings + annual after proxy season",
        "global_real_estate_value_usd": 380_000_000_000_000,
        "global_re_value_confidence": "estimated",
    },

    # ======================================================================
    # US REITS
    # ======================================================================

    "us_reits": {

        # -- Prologis (PLD) ------------------------------------------------
        "pld": {
            "name": "Prologis, Inc.",
            "ticker": "PLD",
            "market_cap_usd": 115_000_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 95_000_000_000,
            "total_assets_confidence": "confirmed",
            "aum_usd": 210_000_000_000,
            "aum_confidence": "confirmed",
            "hq": "San Francisco, CA",
            "property_type": "industrial / logistics",
            "properties_count": 5_600,
            "sqft_owned_managed": 1_200_000_000,
            "sqft_confidence": "confirmed",
            "countries_present": 19,
            "dividend_yield_pct": 2.8,
            "key_personnel": [
                {
                    "name": "Hamid Moghadam",
                    "title": "Chairman & CEO",
                    "since": 2011,
                    "background": "Co-founded AMB Property (merged with ProLogis 2011). Iranian-American, Stanford MBA. Built PLD into largest industrial REIT globally.",
                    "compensation_2024_usd": 25_000_000,
                    "net_worth_est_usd": 1_500_000_000,
                    "pld_shares_value_usd": 800_000_000,
                    "confidence": "confirmed",
                },
                {
                    "name": "Tim Arndt",
                    "title": "CFO",
                    "confidence": "confirmed",
                },
                {
                    "name": "Dan Letter",
                    "title": "President",
                    "note": "Succession candidate, runs global operations",
                    "confidence": "confirmed",
                },
            ],
            "strategic_thesis": (
                "Dominates global warehouse/logistics. E-commerce structural tailwind: "
                "every $1B in online sales requires ~1.2M sqft of warehouse space. "
                "Amazon is largest tenant (~4% of rent). Supply chain reshoring and "
                "nearshoring drives demand. PLD also builds data centers on logistics "
                "land (10GW pipeline announced 2024)."
            ),
            "top_tenants": [
                {"name": "Amazon", "pct_rent": 4.0, "confidence": "confirmed"},
                {"name": "FedEx", "pct_rent": 2.2, "confidence": "confirmed"},
                {"name": "DHL", "pct_rent": 1.8, "confidence": "confirmed"},
                {"name": "Home Depot", "pct_rent": 1.5, "confidence": "confirmed"},
            ],
            "competitive_moat": [
                "Largest global logistics portfolio -- scale advantage in tenant relationships",
                "Embedded land bank (8,000+ acres) for development pipeline",
                "Data center pivot leverages existing industrial zoning + power access",
                "Strategic fund management (co-investment vehicles) amplifies AUM with less balance sheet risk",
            ],
            "risks": [
                "E-commerce growth deceleration post-COVID normalization",
                "New industrial supply deliveries in 2024-2025 pushing vacancy up from 2.5% to 6%+",
                "Data center pivot execution risk -- different expertise than logistics",
                "Concentration in coastal markets with high land costs",
            ],
            "political_connections": {
                "lobbying_spend_2024_usd": 2_100_000,
                "lobbying_confidence": "estimated",
                "pac_contributions_2024_usd": 450_000,
                "pac_confidence": "estimated",
                "key_lobbying_issues": [
                    "REIT tax treatment preservation",
                    "trade / tariff policy (import volumes drive warehouse demand)",
                    "renewable energy tax credits (rooftop solar on warehouses)",
                    "infrastructure spending (roads, ports near logistics parks)",
                ],
                "revolving_door": [
                    {
                        "name": "Various",
                        "note": "PLD board includes former senior government officials; lower profile than financial sector revolving door",
                        "confidence": "inferred",
                    },
                ],
            },
            "offshore_structures": [
                {
                    "detail": "Operates globally through subsidiary entities in Luxembourg, Singapore, Japan, Brazil. Standard REIT multinational structure, not opaque.",
                    "confidence": "confirmed",
                },
            ],
            "insider_trading_patterns": {
                "recent_activity": "Moghadam periodic 10b5-1 sales (~$50M/year). No unusual cluster buys.",
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "risk_level": "low",
                "rationale": "Well-capitalized, investment-grade debt, diversified tenants. Not a systemic risk vector.",
                "confidence": "derived",
            },
        },

        # -- American Tower (AMT) ------------------------------------------
        "amt": {
            "name": "American Tower Corporation",
            "ticker": "AMT",
            "market_cap_usd": 95_000_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 52_000_000_000,
            "total_assets_confidence": "confirmed",
            "hq": "Boston, MA",
            "property_type": "cell towers / telecom infrastructure",
            "towers_owned": 224_000,
            "towers_confidence": "confirmed",
            "countries_present": 25,
            "dividend_yield_pct": 3.2,
            "key_personnel": [
                {
                    "name": "Steven Vondran",
                    "title": "President & CEO",
                    "since": 2024,
                    "background": "Succeeded Tom Bartlett. Previously EVP/COO. Tower industry veteran.",
                    "confidence": "confirmed",
                },
                {
                    "name": "Tom Bartlett",
                    "title": "Former CEO (retired 2024)",
                    "note": "Led AMT through India expansion and CoreSite data center acquisition",
                    "confidence": "confirmed",
                },
                {
                    "name": "Rod Smith",
                    "title": "CFO",
                    "confidence": "confirmed",
                },
            ],
            "strategic_thesis": (
                "Cell tower monopoly economics: 3 carriers (T-Mobile, AT&T, Verizon) "
                "must co-locate on existing towers. 98%+ lease renewal rate. 5G densification "
                "requires MORE tower/small cell sites. ~60% gross margins. India (Vodafone Idea "
                "risk) is main concern. Sold India business (ATC India) to Brookfield 2024."
            ),
            "top_tenants": [
                {"name": "T-Mobile", "pct_revenue": 20, "confidence": "estimated"},
                {"name": "AT&T", "pct_revenue": 17, "confidence": "estimated"},
                {"name": "Verizon", "pct_revenue": 14, "confidence": "estimated"},
            ],
            "competitive_moat": [
                "Natural monopoly: zoning/permitting makes new tower builds slow and expensive",
                "Long-term leases (10-20 year initial terms) with 3% annual escalators",
                "Multi-tenant model: incremental tenant on existing tower is ~80% margin",
                "Global scale in emerging markets where mobile-first internet dominates",
            ],
            "risks": [
                "Carrier consolidation reduces tenant count (T-Mobile/Sprint already digested)",
                "India business was a drag -- sold to Brookfield but transition risks remain",
                "Satellite internet (Starlink, AST SpaceMobile) long-term disruption risk",
                "Interest rate sensitivity -- tower REITs trade like long-duration bonds",
            ],
            "political_connections": {
                "lobbying_spend_2024_usd": 3_800_000,
                "lobbying_confidence": "estimated",
                "key_lobbying_issues": [
                    "FCC spectrum allocation (5G deployment)",
                    "FAA tower siting regulations",
                    "Foreign investment review (CFIUS) for international operations",
                    "Municipal zoning reform for small cell deployment",
                ],
            },
            "offshore_structures": [
                {
                    "detail": "International operations through subsidiaries in India (sold), Mexico, Brazil, Africa, Europe. Standard multinational structure.",
                    "confidence": "confirmed",
                },
            ],
            "insider_trading_patterns": {
                "recent_activity": "Normal executive 10b5-1 plans. No unusual patterns.",
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "risk_level": "low",
                "rationale": "Essential infrastructure, long-term contracted revenue. Interest rate risk is main vector.",
                "confidence": "derived",
            },
        },

        # -- Equinix (EQIX) -----------------------------------------------
        "eqix": {
            "name": "Equinix, Inc.",
            "ticker": "EQIX",
            "market_cap_usd": 85_000_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 32_000_000_000,
            "total_assets_confidence": "confirmed",
            "hq": "Redwood City, CA",
            "property_type": "data centers / digital infrastructure",
            "data_centers": 270,
            "data_centers_confidence": "confirmed",
            "countries_present": 32,
            "customers": 10_000,
            "dividend_yield_pct": 2.0,
            "key_personnel": [
                {
                    "name": "Adaire Fox-Martin",
                    "title": "President & CEO",
                    "since": 2024,
                    "background": "Joined from Google Cloud (president). Previously SAP executive. First female CEO of EQIX.",
                    "confidence": "confirmed",
                },
                {
                    "name": "Charles Meyers",
                    "title": "Former CEO",
                    "note": "Stepped down 2024 after leading massive international expansion",
                    "confidence": "confirmed",
                },
                {
                    "name": "Keith Taylor",
                    "title": "CFO",
                    "since": 2005,
                    "confidence": "confirmed",
                },
            ],
            "strategic_thesis": (
                "World's largest data center REIT and interconnection platform. AI/ML training "
                "and inference driving massive demand for colocation and power. EQIX sits at "
                "network crossroads -- 450,000+ cross-connects. Hyperscalers (AWS, Azure, GCP) "
                "are both customers and competitors. xScale JV program builds hyperscale facilities "
                "with partners (GIC, ADIA). Power access is the new bottleneck."
            ),
            "ai_infrastructure_angle": {
                "relevance": "critical",
                "detail": (
                    "AI training clusters need massive power and interconnection. EQIX provides "
                    "colocation for GPU clusters and low-latency cross-connects between cloud "
                    "providers. xScale program specifically targets hyperscale AI workloads. "
                    "Power procurement (3.5GW+ contracted) is strategic moat."
                ),
                "confidence": "confirmed",
            },
            "competitive_moat": [
                "Network effects: 450K+ cross-connects create switching costs",
                "Global footprint: only data center company in 32 countries",
                "Interconnection revenue (~20% of total) is highest-margin, stickiest",
                "Power procurement at scale in constrained markets",
            ],
            "risks": [
                "Hyperscaler self-build (AWS, Google, Microsoft building own data centers)",
                "Power availability constraints in key markets (Northern Virginia, Singapore, Dublin)",
                "Hindenburg Research short report (2024) alleged accounting manipulation -- EQIX denied",
                "Capital intensity: $3B+/year capex required to maintain growth",
            ],
            "hindenburg_controversy": {
                "date": "2024-03",
                "allegation": "Hindenburg alleged EQIX inflated AFFO by manipulating maintenance vs growth capex classification",
                "eqix_response": "Denied allegations, reaffirmed accounting practices",
                "outcome": "SEC reportedly reviewed; no enforcement action as of early 2026",
                "confidence": "confirmed",
            },
            "political_connections": {
                "lobbying_spend_2024_usd": 1_600_000,
                "lobbying_confidence": "estimated",
                "key_lobbying_issues": [
                    "Data privacy regulations (affects data sovereignty requirements = more local DCs needed)",
                    "Power grid infrastructure investment",
                    "AI regulation (indirect: more regulation = more compliance compute)",
                    "REIT tax treatment preservation",
                ],
            },
            "offshore_structures": [
                {
                    "detail": "Operations in 32 countries through local subsidiaries. Standard multinational REIT structure.",
                    "confidence": "confirmed",
                },
            ],
            "systemic_risk": {
                "risk_level": "moderate",
                "rationale": "Critical internet infrastructure. If EQIX went offline, significant portion of global internet traffic would be disrupted. Single-name concentration risk for cloud ecosystem.",
                "confidence": "derived",
            },
        },

        # -- Simon Property Group (SPG) ------------------------------------
        "spg": {
            "name": "Simon Property Group, Inc.",
            "ticker": "SPG",
            "market_cap_usd": 58_000_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 40_000_000_000,
            "total_assets_confidence": "confirmed",
            "hq": "Indianapolis, IN",
            "property_type": "malls / premium outlets / mixed-use",
            "properties_count": 195,
            "properties_confidence": "confirmed",
            "dividend_yield_pct": 4.8,
            "key_personnel": [
                {
                    "name": "David Simon",
                    "title": "Chairman, CEO & President",
                    "since": 1995,
                    "background": (
                        "Son of Melvin Simon (co-founder). MBA Wharton. Has run SPG for 30 years. "
                        "Ruthless operator -- cut dividend during COVID (first REIT to do so), then "
                        "bought distressed retail assets (JCPenney, Brooks Brothers, Forever 21, "
                        "Lucky Brand) via joint venture with Brookfield (SPARC Group). "
                        "Known for hostile opposition to activist investors."
                    ),
                    "compensation_2024_usd": 23_000_000,
                    "net_worth_est_usd": 4_000_000_000,
                    "spg_shares_value_usd": 2_500_000_000,
                    "insider_ownership_pct": 8.5,
                    "confidence": "confirmed",
                },
                {
                    "name": "Brian McDade",
                    "title": "CFO",
                    "confidence": "confirmed",
                },
            ],
            "strategic_thesis": (
                "Largest US mall REIT. Post-COVID thesis: A-malls survive, B/C malls die. "
                "SPG owns the best malls (Premium Outlets, Mills). Occupancy recovered to 95%+. "
                "Mixed-use densification strategy: adding residential, hotel, office to mall parcels. "
                "International via joint ventures in Europe, Asia. David Simon is the franchise."
            ),
            "competitive_moat": [
                "Owns irreplaceable Class A mall locations in top 50 MSAs",
                "Premium Outlets brand -- highest sales PSF in outlet sector ($750+/sqft)",
                "Scale: largest landlord for major retailers = negotiating leverage",
                "Mixed-use redevelopment pipeline converts dying retail into mixed-use",
            ],
            "risks": [
                "Secular decline in physical retail (long-term e-commerce shift)",
                "SPARC Group retailer investments (JCPenney etc) are capital-intensive turnarounds",
                "David Simon key-man risk -- no clear succession plan",
                "Rising insurance and property tax costs in top markets",
            ],
            "political_connections": {
                "lobbying_spend_2024_usd": 1_800_000,
                "lobbying_confidence": "estimated",
                "pac_contributions_2024_usd": 680_000,
                "pac_confidence": "estimated",
                "key_lobbying_issues": [
                    "REIT tax treatment",
                    "Retail sales tax parity (online vs brick-and-mortar)",
                    "Municipal zoning for mixed-use redevelopment",
                    "Immigration policy (labor availability for retail tenants)",
                ],
                "david_simon_political": {
                    "note": "David Simon is a major Republican donor but pragmatic -- donates to both parties for local zoning/permitting influence",
                    "confidence": "estimated",
                },
            },
            "offshore_structures": [
                {
                    "detail": "International JVs in Europe (Klepierre stake sold), Asia (joint ventures in Japan, South Korea, Malaysia). Standard structure.",
                    "confidence": "confirmed",
                },
            ],
            "insider_trading_patterns": {
                "recent_activity": "David Simon buys on dips -- added ~$50M in 2020 COVID crash. Strong insider alignment.",
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "risk_level": "low",
                "rationale": "Well-capitalized, investment-grade. Not systemic. Risk is secular, not acute.",
                "confidence": "derived",
            },
        },

        # -- Realty Income (O) ---------------------------------------------
        "o": {
            "name": "Realty Income Corporation",
            "ticker": "O",
            "market_cap_usd": 50_000_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 62_000_000_000,
            "total_assets_confidence": "confirmed",
            "hq": "San Diego, CA",
            "property_type": "triple net lease (NNN)",
            "properties_count": 15_400,
            "properties_confidence": "confirmed",
            "countries_present": 5,
            "dividend_yield_pct": 5.5,
            "monthly_dividend": True,
            "consecutive_dividend_increases": 107,
            "key_personnel": [
                {
                    "name": "Sumit Roy",
                    "title": "President & CEO",
                    "since": 2018,
                    "background": "Joined Realty Income 2011. Previously at UBS and Bear Stearns. Led Spirit Realty merger (2024).",
                    "compensation_2024_usd": 15_000_000,
                    "confidence": "confirmed",
                },
                {
                    "name": "Jonathan Pong",
                    "title": "CFO",
                    "confidence": "confirmed",
                },
            ],
            "strategic_thesis": (
                "The 'Monthly Dividend Company' -- defensive REIT focused on single-tenant "
                "triple-net-lease properties (tenant pays taxes, insurance, maintenance). "
                "15,400+ properties, 85%+ investment-grade tenants. Acquired Spirit Realty 2024 "
                "($9.3B). Expanding into Europe and gaming/casino NNN. Retail investors love "
                "the monthly dividend (107 consecutive quarterly increases = 30+ years)."
            ),
            "top_tenants": [
                {"name": "Dollar General", "pct_rent": 3.4, "confidence": "confirmed"},
                {"name": "Walgreens", "pct_rent": 3.3, "confidence": "confirmed"},
                {"name": "Dollar Tree / Family Dollar", "pct_rent": 3.1, "confidence": "confirmed"},
                {"name": "7-Eleven", "pct_rent": 2.5, "confidence": "confirmed"},
                {"name": "FedEx", "pct_rent": 2.1, "confidence": "confirmed"},
                {"name": "Wynn Resorts (Encore Boston)", "pct_rent": 1.8, "note": "casino NNN entry", "confidence": "confirmed"},
            ],
            "competitive_moat": [
                "Scale: largest NNN REIT = best access to deal flow",
                "Investment-grade balance sheet enables lowest cost of capital in sector",
                "Monthly dividend culture creates retail investor loyalty (stock as bond substitute)",
                "Diversification: 15K+ properties across 50 states, no single tenant >3.5%",
            ],
            "risks": [
                "Tenant credit risk: Walgreens/Dollar stores facing operational challenges",
                "Interest rate sensitivity: NNN REITs trade inversely with rates",
                "Spirit Realty integration: dilutive if portfolio quality is lower than legacy O",
                "Europe expansion (UK, Spain, Italy) introduces FX and regulatory risk",
            ],
            "political_connections": {
                "lobbying_spend_2024_usd": 800_000,
                "lobbying_confidence": "estimated",
                "key_lobbying_issues": [
                    "REIT tax treatment preservation",
                    "1031 exchange rules (critical for NNN acquisitions)",
                    "Carried interest taxation",
                ],
            },
            "systemic_risk": {
                "risk_level": "low",
                "rationale": "Diversified, investment-grade, long-dated leases. Rate-sensitive but not systemic.",
                "confidence": "derived",
            },
        },

        # -- Vornado Realty Trust (VNO) ------------------------------------
        "vno": {
            "name": "Vornado Realty Trust",
            "ticker": "VNO",
            "market_cap_usd": 7_500_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 16_000_000_000,
            "total_assets_confidence": "confirmed",
            "hq": "New York, NY",
            "property_type": "NYC office / street retail",
            "properties_count": 35,
            "properties_confidence": "confirmed",
            "dividend_yield_pct": 2.0,
            "dividend_note": "Cut dividend 2023 during office crisis, partially restored 2024",
            "key_personnel": [
                {
                    "name": "Steven Roth",
                    "title": "Chairman & CEO",
                    "since": 1980,
                    "background": (
                        "Founded Vornado in 1980 (originally Interstate Properties). Built it into "
                        "premier NYC office landlord. Close personal friend of Jared Kushner. "
                        "Roth and Kushner families have co-invested on multiple deals. "
                        "Roth was member of Trump's infrastructure advisory council (2017, disbanded). "
                        "Major NYC political power broker -- relationships with every NYC mayor."
                    ),
                    "net_worth_est_usd": 2_000_000_000,
                    "vno_shares_value_usd": 900_000_000,
                    "compensation_2024_usd": 10_000_000,
                    "trump_connections": {
                        "detail": (
                            "Roth was close to Trump Organization pre-presidency. Sat on Trump's "
                            "infrastructure advisory council 2017. Kushner Companies and Vornado "
                            "co-own 666 Fifth Avenue and 1290 Avenue of the Americas as JV partners. "
                            "Roth has donated to both parties but has strong Republican/Trump orbit ties."
                        ),
                        "confidence": "confirmed",
                    },
                    "confidence": "confirmed",
                },
                {
                    "name": "Michael Franco",
                    "title": "President & CFO",
                    "confidence": "confirmed",
                },
                {
                    "name": "Glen Weiss",
                    "title": "Co-Head of Real Estate, Executive VP",
                    "confidence": "confirmed",
                },
            ],
            "strategic_thesis": (
                "Concentrated bet on NYC office. PENN District redevelopment ($3B+ Vornado spend "
                "near Penn Station) is the make-or-break project. If NYC office recovers + Penn "
                "Station renovation happens, VNO re-rates dramatically. If WFH persists and "
                "PENN stalls, VNO equity is impaired. Binary outcome."
            ),
            "penn_district_project": {
                "total_investment_usd": 3_000_000_000,
                "status": "under construction / phased",
                "detail": (
                    "Redeveloping blocks around Penn Station into modern office/retail/transit hub. "
                    "Dependent on NYS Penn Station renovation (stalled). If the area transforms, "
                    "VNO assets re-rate; if not, capital is trapped."
                ),
                "confidence": "confirmed",
            },
            "kushner_vornado_jv": {
                "properties": [
                    {"address": "666 Fifth Avenue, NYC", "note": "Kushner Cos bought 2007 for $1.8B (record price). Refinanced with Brookfield. Vornado holds 49.5% of retail condo."},
                    {"address": "1290 Avenue of the Americas, NYC", "note": "Vornado-Kushner JV. Kushner Cos sold stake to reduce debt."},
                ],
                "political_angle": "Kushner family RE dealings attracted scrutiny during Trump presidency. Brookfield's $1.1B 99-year lease on 666 Fifth (2018) raised questions about Qatari sovereign wealth fund (QIA) connections via Brookfield.",
                "confidence": "confirmed",
            },
            "political_connections": {
                "lobbying_spend_2024_usd": 1_200_000,
                "lobbying_confidence": "estimated",
                "key_lobbying_issues": [
                    "Penn Station renovation (federal + state funding)",
                    "NYC zoning reform (office-to-residential conversion)",
                    "Tax abatements for NYC office development",
                    "Federal office space leasing (GSA)",
                ],
                "political_donations": {
                    "steve_roth": "Donates to both parties. Historically Republican-leaning but pragmatic NYC operator.",
                    "confidence": "confirmed",
                },
            },
            "risks": [
                "NYC office market: 22% vacancy, worst since 1990s",
                "PENN District execution risk -- $3B bet on neighborhood transformation",
                "WFH secular shift reducing office demand permanently",
                "Dividend already cut once; further cuts possible if occupancy doesn't recover",
                "Key-man risk: Steve Roth is 83 years old, no clear succession",
            ],
            "systemic_risk": {
                "risk_level": "moderate",
                "rationale": "Concentrated NYC office exposure. Not globally systemic but emblematic of broader CRE office crisis.",
                "confidence": "derived",
            },
        },

        # -- Blackstone BREIT (non-traded) ---------------------------------
        "breit": {
            "name": "Blackstone Real Estate Income Trust (BREIT)",
            "ticker": "BREIT (non-traded)",
            "market_cap_usd": None,
            "nav_usd": 60_000_000_000,
            "nav_confidence": "estimated",
            "aum_usd": 60_000_000_000,
            "aum_confidence": "estimated",
            "hq": "New York, NY (Blackstone)",
            "property_type": "diversified (residential, industrial, hospitality, data centers)",
            "structure": "non-traded REIT (limited liquidity)",
            "key_personnel": [
                {
                    "name": "Jon Gray",
                    "title": "Blackstone President & COO (oversees RE globally)",
                    "background": (
                        "Architect of Blackstone's real estate empire. Joined Blackstone 1992. "
                        "Built RE platform from ~$5B to $330B+ AUM. Most important figure in "
                        "private real estate globally. Likely successor to Steve Schwarzman."
                    ),
                    "net_worth_est_usd": 7_000_000_000,
                    "confidence": "estimated",
                },
                {
                    "name": "Nadeem Meghji",
                    "title": "Head of Blackstone Real Estate Americas",
                    "confidence": "confirmed",
                },
                {
                    "name": "Jacob Werner",
                    "title": "Head of Blackstone Real Estate Europe",
                    "confidence": "confirmed",
                },
            ],
            "strategic_thesis": (
                "BREIT is Blackstone's retail-facing non-traded REIT. Offers monthly NAV-based "
                "pricing (not mark-to-market daily). Attracted $70B+ from retail/wealth channels. "
                "Controversy: redemption gates hit in late 2022 when investors rushed to exit. "
                "Blackstone limited withdrawals to 2%/month and 5%/quarter. UC system investment "
                "($4B from Regents of UC) was criticized as bailout optics."
            ),
            "redemption_crisis": {
                "date_started": "2022-11",
                "detail": (
                    "Investors requested $4B+ in redemptions in Nov 2022 alone, exceeding gate limits. "
                    "BREIT imposed 2%/month, 5%/quarter redemption caps. Asian investors (particularly "
                    "Hong Kong/Singapore wealth channels) were largest redeemers. Gates remained active "
                    "through mid-2023. University of California Regents invested $4B in Jan 2023, "
                    "interpreted by some as a confidence signal and by critics as a sweetheart deal "
                    "to stem outflows."
                ),
                "uc_investment": {
                    "amount_usd": 4_000_000_000,
                    "date": "2023-01",
                    "terms": "Preferred return + ability to share in future appreciation. Criticized as sweetheart terms.",
                    "jagdeep_bachher": "UC CIO who approved deal. Previously at Alberta Investment Management.",
                    "controversy": "Some UC faculty/students protested. SEC did not take action.",
                    "confidence": "confirmed",
                },
                "gates_lifted": "2023-Q3 (approximately)",
                "confidence": "confirmed",
            },
            "portfolio_composition": {
                "residential_multifamily_pct": 35,
                "industrial_logistics_pct": 25,
                "data_centers_pct": 20,
                "hospitality_pct": 10,
                "other_pct": 10,
                "confidence": "estimated",
            },
            "risks": [
                "Liquidity mismatch: illiquid real estate in a daily/monthly redemption vehicle",
                "NAV self-valuation: Blackstone appraises its own assets quarterly (conflict of interest)",
                "Redemption gates can return if sentiment shifts",
                "Retail investors may not understand non-traded REIT illiquidity",
                "Regulatory scrutiny of non-traded REIT sales practices",
            ],
            "political_connections": {
                "note": "See Blackstone Real Estate entry in private_real_estate section for full political profile",
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "risk_level": "moderate",
                "rationale": (
                    "Not systemic alone, but emblematic of liquidity mismatch in private RE vehicles. "
                    "If BREIT gates returned + Starwood SREIT + other non-traded REITs simultaneously "
                    "gated, it would signal broader private RE distress and trigger sentiment contagion."
                ),
                "confidence": "derived",
            },
        },
    },

    # ======================================================================
    # GLOBAL PROPERTY EMPIRES
    # ======================================================================

    # -- UAE Royal Family London Property ----------------------------------
    "uae_royal_property": {
        "khalifa_bin_zayed_estate": {
            "name": "Estate of Khalifa bin Zayed Al Nahyan (deceased 2022)",
            "entity_type": "sovereign / royal family property empire",
            "country": "UAE (Abu Dhabi)",
            "estimated_london_property_usd": 7_000_000_000,
            "estimated_global_property_usd": 15_000_000_000,
            "confidence": "estimated",
            "panama_papers_exposure": {
                "detail": (
                    "ICIJ Panama Papers (2016) revealed Khalifa bin Zayed personally owned "
                    "at least 30 London properties worth ~$1.7B at time of leak, held through "
                    "shell companies in the British Virgin Islands (BVI) set up by Mossack Fonseca. "
                    "Properties included: Mayfair townhouses, Belgravia mansions, Knightsbridge "
                    "penthouses, and a 97-acre estate in Ascot, Berkshire."
                ),
                "shell_companies": [
                    "Multiple BVI-registered entities (exact names in ICIJ database)",
                    "Administered by Mossack Fonseca Geneva office",
                    "Nominee directors used to obscure beneficial ownership",
                ],
                "post_leak_status": "UK began Unexplained Wealth Orders (UWOs) but none applied to UAE royals. Political sensitivity.",
                "confidence": "confirmed",
            },
            "key_people": [
                {
                    "name": "Sheikh Khalifa bin Zayed Al Nahyan",
                    "title": "Former President of UAE (d. 2022)",
                    "note": "Properties now controlled by family/estate trustees. Succeeded by Mohamed bin Zayed (MBZ).",
                    "confidence": "confirmed",
                },
                {
                    "name": "Mohamed bin Zayed Al Nahyan (MBZ)",
                    "title": "Current President of UAE",
                    "note": "Controls Abu Dhabi Investment Authority (ADIA, $993B) and Mubadala ($302B). Real estate is a fraction of total wealth.",
                    "confidence": "confirmed",
                },
            ],
            "adia_real_estate": {
                "estimated_re_allocation_usd": 100_000_000_000,
                "allocation_pct": 10,
                "detail": "ADIA allocates ~10% to real estate. Major stakes in Equinix xScale JVs, US multifamily, European logistics, Indian commercial RE.",
                "confidence": "estimated",
            },
            "mubadala_real_estate": {
                "estimated_re_allocation_usd": 25_000_000_000,
                "detail": "Mubadala invests in real estate through its alternatives arm. Key markets: US, UK, Singapore.",
                "confidence": "estimated",
            },
            "political_connections": {
                "uk_relationship": "UAE is top arms customer of UK. Property ownership creates soft power in London. UK reluctant to apply UWOs to Gulf royals.",
                "us_relationship": "MBZ is key US ally. ADIA/Mubadala are major investors in US real estate and infrastructure.",
                "confidence": "derived",
            },
            "systemic_risk": {
                "risk_level": "low",
                "rationale": "Sovereign wealth = patient capital. Not leveraged. Risk is political (sanctions, transparency) not financial.",
                "confidence": "derived",
            },
        },
    },

    # -- Chinese Developers ------------------------------------------------
    "chinese_developers": {
        "evergrande": {
            "name": "China Evergrande Group",
            "ticker": "3333.HK (suspended)",
            "status": "liquidation ordered Jan 2024",
            "total_liabilities_usd": 340_000_000_000,
            "total_liabilities_confidence": "confirmed",
            "key_personnel": [
                {
                    "name": "Hui Ka Yan (Xu Jiayin)",
                    "title": "Founder & Former Chairman",
                    "status": "Detained by Chinese authorities (Sep 2023). Under criminal investigation.",
                    "peak_net_worth_usd": 42_000_000_000,
                    "current_net_worth_est_usd": 0,
                    "confidence": "confirmed",
                },
            ],
            "timeline": [
                {"date": "2021-09", "event": "First missed bond payment, triggering global contagion fears"},
                {"date": "2021-12", "event": "Officially defaulted on offshore bonds"},
                {"date": "2023-09", "event": "Hui Ka Yan detained by police"},
                {"date": "2024-01", "event": "Hong Kong court ordered liquidation"},
                {"date": "2024-2025", "event": "Liquidators attempting to recover assets. Minimal recovery expected for offshore creditors."},
            ],
            "contagion_effects": [
                "Triggered broader Chinese property sector crisis",
                "Offshore bond market for Chinese developers effectively closed",
                "Pre-sold apartment buyers left with unfinished homes (social stability risk)",
                "Local government land sales revenue collapsed (~40% of LGFV income)",
                "Global investors (Ashmore, PIMCO, BlackRock) took significant losses",
            ],
            "confidence": "confirmed",
        },
        "country_garden": {
            "name": "Country Garden Holdings",
            "ticker": "2007.HK (suspended)",
            "status": "defaulted 2023, restructuring",
            "total_liabilities_usd": 200_000_000_000,
            "total_liabilities_confidence": "estimated",
            "key_personnel": [
                {
                    "name": "Yang Huiyan",
                    "title": "Chairperson",
                    "background": "Daughter of founder Yang Guoqiang. Was China's richest woman. Wealth collapsed from $30B to near zero.",
                    "peak_net_worth_usd": 30_000_000_000,
                    "current_net_worth_est_usd": 1_000_000_000,
                    "confidence": "estimated",
                },
            ],
            "status_detail": (
                "Defaulted on offshore bonds Aug 2023. Larger than Evergrande by units sold. "
                "Considered 'too big to fail' as it was historically seen as a well-run private "
                "developer (vs Evergrande's excess). Default shattered remaining confidence "
                "in Chinese property sector."
            ),
            "confidence": "confirmed",
        },
        "vanke": {
            "name": "China Vanke Co.",
            "ticker": "000002.SZ / 2202.HK",
            "status": "distressed but operating (early 2026)",
            "market_cap_usd": 8_000_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 250_000_000_000,
            "total_assets_confidence": "estimated",
            "key_personnel": [
                {
                    "name": "Yu Liang",
                    "title": "Former Chairman (resigned under pressure 2024)",
                    "note": "Detained by authorities in connection with corruption investigation",
                    "confidence": "confirmed",
                },
                {
                    "name": "Shenzhen Metro Group",
                    "title": "Largest shareholder (27%)",
                    "note": "State-owned enterprise. Vanke is quasi-SOE, which should provide implicit support but hasn't fully prevented distress.",
                    "confidence": "confirmed",
                },
            ],
            "strategic_context": (
                "Vanke was considered China's best-run developer -- prudent, transparent, "
                "good governance. Its distress signals that the Chinese property downturn is "
                "structural, not just about bad actors. Shenzhen Metro backstop provides some "
                "floor but Vanke's bond spreads remain distressed (800bps+ over benchmark)."
            ),
            "confidence": "confirmed",
        },
        "china_property_crisis_summary": {
            "total_developer_debt_usd": 5_000_000_000_000,
            "total_developer_debt_confidence": "estimated",
            "defaulted_developers_count": 50,
            "defaulted_developers_confidence": "estimated",
            "home_price_decline_from_peak_pct": -25,
            "home_price_confidence": "estimated",
            "new_starts_decline_from_peak_pct": -60,
            "new_starts_confidence": "estimated",
            "lgfv_exposure": {
                "detail": "Local government financing vehicles relied on land sales for 30-40% of revenue. Property downturn creates fiscal crisis for Chinese local governments.",
                "estimated_lgfv_debt_usd": 9_000_000_000_000,
                "confidence": "estimated",
            },
            "policy_response": [
                "PBOC rate cuts and mortgage rate reductions",
                "'Three arrows' policy: bank loans, bond issuance, equity financing for developers",
                "Local government property purchase programs (buying unsold inventory)",
                "Relaxation of purchase restrictions in most cities",
                "Effect: stabilized top-tier cities (Beijing, Shanghai) but lower-tier cities still falling",
            ],
            "global_contagion_risk": {
                "risk_level": "high",
                "detail": (
                    "Chinese property is ~30% of GDP. Prolonged downturn affects global commodity "
                    "demand (iron ore, copper, cement), luxury goods, and sentiment. Offshore bond "
                    "losses hit global EM debt funds. Japanese and Korean banks have China RE exposure."
                ),
                "confidence": "derived",
            },
        },
    },

    # -- Hong Kong Tycoons -------------------------------------------------
    "hk_tycoons": {
        "li_ka_shing": {
            "name": "Li Ka-shing",
            "title": "Founder, CK Hutchison / CK Asset",
            "net_worth_usd": 35_000_000_000,
            "net_worth_confidence": "estimated",
            "age": 97,
            "key_entities": [
                {
                    "name": "CK Asset Holdings",
                    "ticker": "1113.HK",
                    "market_cap_usd": 15_000_000_000,
                    "focus": "Hong Kong and global real estate",
                    "confidence": "confirmed",
                },
                {
                    "name": "CK Hutchison Holdings",
                    "ticker": "0001.HK",
                    "market_cap_usd": 20_000_000_000,
                    "focus": "Ports, retail, telecom, infrastructure (includes property)",
                    "confidence": "confirmed",
                },
                {
                    "name": "Li Ka Shing Foundation",
                    "assets_usd": 3_000_000_000,
                    "focus": "Education, healthcare. Also strategic tech investments (Facebook early, Zoom, DeepMind pre-Google).",
                    "confidence": "estimated",
                },
            ],
            "background": (
                "The 'Superman' of Hong Kong. Refugee from Chaozhou, China. Built empire from "
                "plastic flowers factory. Richest person in Asia for decades. Master of buying "
                "distressed assets and selling at peaks. Sold much of his China/HK property "
                "2013-2015 (prescient) and invested heavily in UK/Europe infrastructure."
            ),
            "political_connections": {
                "hk_relationship": "Historically close to every HK Chief Executive. Relationship with Beijing cooled after 2019 protests (perceived as insufficiently supportive).",
                "beijing_relationship": "Complicated. Li diversified OUT of China aggressively, which Beijing noticed. Still maintains relationships but no longer 'red capital'.",
                "uk_relationship": "Massive UK infrastructure owner (Three UK mobile, Northumbrian Water, UK Power Networks). Sold some UK assets 2024-2025 amid UK regulatory tightening.",
                "confidence": "derived",
            },
            "offshore_structures": [
                {
                    "detail": "Empire run through Cayman Islands holding companies (standard for HK conglomerates). Li family trusts are structured through offshore vehicles.",
                    "confidence": "confirmed",
                },
            ],
            "succession": {
                "successor": "Victor Li (elder son) -- Chairman of CK Hutchison and CK Asset",
                "other_son": "Richard Li -- runs PCCW (telecom) separately. Less involved in property empire.",
                "confidence": "confirmed",
            },
        },
        "henry_cheng": {
            "name": "Henry Cheng Kar-shun",
            "title": "Chairman, Chow Tai Fook Jewellery / New World Development",
            "net_worth_usd": 25_000_000_000,
            "net_worth_confidence": "estimated",
            "key_entities": [
                {
                    "name": "New World Development",
                    "ticker": "0017.HK",
                    "market_cap_usd": 5_000_000_000,
                    "focus": "Hong Kong and mainland China property, infrastructure, hotels",
                    "confidence": "confirmed",
                },
                {
                    "name": "Chow Tai Fook Jewellery",
                    "ticker": "1929.HK",
                    "market_cap_usd": 15_000_000_000,
                    "focus": "Largest jewelry retailer in Asia. Property is secondary wealth driver.",
                    "confidence": "confirmed",
                },
                {
                    "name": "NWS Holdings",
                    "ticker": "0659.HK",
                    "focus": "Infrastructure, aviation (Goshawk Aviation)",
                    "confidence": "confirmed",
                },
            ],
            "background": (
                "Son of Cheng Yu-tung. Third-generation wealth. Family controls Chow Tai Fook "
                "Enterprises, the holding company above listed entities. Conservative operator "
                "compared to peers but also caught in HK/China property downturn."
            ),
            "succession": {
                "next_gen": "Adrian Cheng (son) -- CEO of New World Development. Younger, more design-forward (K11 Musea luxury mall brand). Harvard MBA.",
                "confidence": "confirmed",
            },
            "political_connections": {
                "beijing_relationship": "Strong. Family is part of HK establishment 'patriotic capitalists'. Henry Cheng is member of CPPCC.",
                "confidence": "confirmed",
            },
        },
        "lee_shau_kee": {
            "name": "Lee Shau-kee",
            "title": "Founder, Henderson Land Development",
            "net_worth_usd": 28_000_000_000,
            "net_worth_confidence": "estimated",
            "age": 97,
            "key_entities": [
                {
                    "name": "Henderson Land Development",
                    "ticker": "0012.HK",
                    "market_cap_usd": 12_000_000_000,
                    "focus": "Hong Kong residential and commercial property",
                    "confidence": "confirmed",
                },
                {
                    "name": "Henderson Investment (via Miramar Hotel)",
                    "focus": "Hotels, retail",
                    "confidence": "confirmed",
                },
            ],
            "background": (
                "Known as 'Hong Kong's Warren Buffett' for his frugality and value investing. "
                "Co-founded New World Development with Cheng Yu-tung before going independent. "
                "Largest individual landowner in Hong Kong. Controversial for contributing to "
                "HK housing affordability crisis (land banking)."
            ),
            "succession": {
                "successors": ["Martin Lee Ka-shing (son) -- Co-Chairman Henderson Land", "Peter Lee Ka-kit (son) -- Co-Chairman Henderson Land"],
                "confidence": "confirmed",
            },
            "political_connections": {
                "beijing_relationship": "Strong Beijing ties. Donated HK$1B to education charities in mainland China. Member of CPPCC.",
                "hk_housing_politics": "Criticized for contributing to housing crisis through land banking. HK government reluctant to act against major property families.",
                "confidence": "derived",
            },
        },
    },

    # -- Singapore REITs ---------------------------------------------------
    "singapore_reits": {
        "capitaland": {
            "name": "CapitaLand Investment (CLI)",
            "ticker": "9CI.SI",
            "market_cap_usd": 14_000_000_000,
            "market_cap_confidence": "estimated",
            "aum_usd": 100_000_000_000,
            "aum_confidence": "estimated",
            "hq": "Singapore",
            "key_personnel": [
                {
                    "name": "Lee Chee Koon",
                    "title": "Group CEO",
                    "confidence": "confirmed",
                },
            ],
            "background": (
                "Largest diversified real estate group in Asia by AUM. Restructured in 2021: "
                "spun off development into CapitaLand Development (privatized by Temasek) and "
                "listed CapitaLand Investment as asset-light fund manager. Temasek Holdings "
                "(Singapore sovereign wealth) is controlling shareholder (~52%)."
            ),
            "reits_managed": [
                {"name": "CapitaLand Integrated Commercial Trust (CICT)", "ticker": "C38U.SI", "aum_usd": 24_000_000_000},
                {"name": "CapitaLand Ascendas REIT", "ticker": "A17U.SI", "aum_usd": 16_000_000_000},
                {"name": "Ascott Residence Trust", "ticker": "HMN.SI", "aum_usd": 8_000_000_000},
            ],
            "temasek_connection": {
                "ownership_pct": 52,
                "detail": "Temasek Holdings (Singapore SWF, $382B AUM) is majority owner. CapitaLand is effectively a sovereign-backed real estate platform.",
                "confidence": "confirmed",
            },
            "political_connections": {
                "singapore_government": "Deeply embedded in Singapore government ecosystem via Temasek. Former senior civil servants on board.",
                "confidence": "confirmed",
            },
        },
        "mapletree": {
            "name": "Mapletree Investments",
            "hq": "Singapore",
            "aum_usd": 78_000_000_000,
            "aum_confidence": "estimated",
            "structure": "Wholly owned by Temasek Holdings",
            "key_personnel": [
                {
                    "name": "Hiew Yoon Khong",
                    "title": "Group CEO (former; transitioned to advisory 2024)",
                    "confidence": "confirmed",
                },
            ],
            "reits_managed": [
                {"name": "Mapletree Pan Asia Commercial Trust", "ticker": "N2IU.SI", "aum_usd": 17_000_000_000},
                {"name": "Mapletree Industrial Trust", "ticker": "ME8U.SI", "aum_usd": 9_000_000_000},
                {"name": "Mapletree Logistics Trust", "ticker": "M44U.SI", "aum_usd": 13_000_000_000},
            ],
            "temasek_connection": {
                "ownership": "100% owned by Temasek",
                "detail": "Pure sovereign wealth fund real estate arm. Operates with SOE-level patient capital.",
                "confidence": "confirmed",
            },
        },
    },

    # -- Japanese REITs ----------------------------------------------------
    "japanese_reits": {
        "mitsui_fudosan": {
            "name": "Mitsui Fudosan Co., Ltd.",
            "ticker": "8801.T",
            "market_cap_usd": 35_000_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 65_000_000_000,
            "total_assets_confidence": "estimated",
            "hq": "Tokyo, Japan",
            "background": (
                "Japan's largest real estate company by revenue. Part of the Mitsui keiretsu "
                "(zaibatsu lineage). Diversified: office, residential, retail, logistics, resorts. "
                "Flagship properties: Tokyo Midtown, LaLaport malls, Mitsui Garden Hotels. "
                "Growing US presence (55 Hudson Yards NYC, co-developed with Related Companies)."
            ),
            "key_personnel": [
                {
                    "name": "Masanobu Komoda",
                    "title": "President & CEO",
                    "confidence": "confirmed",
                },
            ],
            "keiretsu_connections": {
                "detail": "Part of Mitsui Group. Cross-shareholdings with Mitsui & Co (trading), Mitsui Sumitomo (banking/insurance). Keiretsu relationships provide deal flow and cheap financing.",
                "confidence": "confirmed",
            },
            "jreit_sponsored": {
                "name": "Nippon Building Fund (NBF)",
                "ticker": "8951.T",
                "note": "Japan's first J-REIT (listed 2001). Mitsui Fudosan is sponsor.",
                "confidence": "confirmed",
            },
            "boj_jreit_buying": {
                "detail": "Bank of Japan bought J-REIT ETFs as part of monetary easing (2010-2024). BOJ holds ~6% of J-REIT market. Taper risk if BOJ normalizes.",
                "confidence": "confirmed",
            },
        },
        "mitsubishi_estate": {
            "name": "Mitsubishi Estate Co., Ltd.",
            "ticker": "8802.T",
            "market_cap_usd": 25_000_000_000,
            "market_cap_confidence": "estimated",
            "total_assets_usd": 55_000_000_000,
            "total_assets_confidence": "estimated",
            "hq": "Tokyo, Japan",
            "background": (
                "Japan's second-largest developer. Owns/manages the Marunouchi district "
                "(Tokyo's premier office area, in front of Tokyo Station). Part of Mitsubishi "
                "keiretsu. Owns Rockefeller Center (via 1989 acquisition, restructured). "
                "Growing logistics and residential segments."
            ),
            "key_personnel": [
                {
                    "name": "Shunsuke Nakamura",
                    "title": "President & CEO",
                    "confidence": "confirmed",
                },
            ],
            "rockefeller_center": {
                "detail": "Mitsubishi Estate bought controlling stake in Rockefeller Center 1989 for $1.4B (peak of Japan bubble). Lost money, restructured. Still holds significant NYC real estate.",
                "confidence": "confirmed",
            },
            "keiretsu_connections": {
                "detail": "Part of Mitsubishi Group. Cross-shareholdings with MUFG (banking), Mitsubishi Corp (trading), Mitsubishi Heavy Industries.",
                "confidence": "confirmed",
            },
        },
    },

    # ======================================================================
    # PRIVATE REAL ESTATE
    # ======================================================================

    "private_real_estate": {

        # -- Blackstone Real Estate ----------------------------------------
        "blackstone_re": {
            "name": "Blackstone Real Estate",
            "parent": "Blackstone Inc. (BX)",
            "parent_ticker": "BX",
            "aum_usd": 340_000_000_000,
            "aum_confidence": "confirmed",
            "hq": "New York, NY",
            "description": "Largest private real estate investor in the world",
            "key_personnel": [
                {
                    "name": "Stephen Schwarzman",
                    "title": "Chairman, CEO & Co-Founder of Blackstone",
                    "net_worth_usd": 42_000_000_000,
                    "net_worth_confidence": "estimated",
                    "background": (
                        "Co-founded Blackstone 1985 with Pete Peterson ($400K each). "
                        "Built it into world's largest alternative asset manager ($1T+ AUM). "
                        "Major Republican donor. Close to Trump: hosted fundraisers, donated "
                        "$20M+ to Trump-aligned super PACs. Also donates to MIT, Yale, Oxford. "
                        "Schwarzman Scholars program at Tsinghua (China)."
                    ),
                    "political_connections": {
                        "trump_relationship": "Close. Schwarzman was chair of Trump Strategic and Policy Forum (2017, disbanded). Major donor to Trump campaigns and affiliated PACs.",
                        "gop_donations_est_usd": 50_000_000,
                        "gop_donations_confidence": "estimated",
                        "china_relationship": "Schwarzman Scholars at Tsinghua University. Personal relationships with Chinese leaders. Blackstone was early Western investor in China.",
                        "confidence": "confirmed",
                    },
                    "confidence": "confirmed",
                },
                {
                    "name": "Jon Gray",
                    "title": "President & COO, Blackstone Inc. (former Head of RE)",
                    "net_worth_usd": 7_000_000_000,
                    "background": "Built Blackstone RE from ~$5B to $330B+. Made Blackstone the #1 name in private RE. Key deals: Hilton ($26B, 2007), Equity Office Properties ($39B, 2007), Logicor (European logistics), QTS Realty (data centers).",
                    "net_worth_confidence": "estimated",
                    "confidence": "confirmed",
                },
                {
                    "name": "Kathleen McCarthy",
                    "title": "Global Co-Head of Blackstone Real Estate",
                    "confidence": "confirmed",
                },
            ],
            "landmark_deals": [
                {
                    "deal": "Equity Office Properties (EOP)",
                    "year": 2007,
                    "price_usd": 39_000_000_000,
                    "note": "Largest LBO in history at the time. Blackstone flipped most assets before GFC hit. Legendary timing.",
                    "confidence": "confirmed",
                },
                {
                    "deal": "Hilton Hotels",
                    "year": 2007,
                    "price_usd": 26_000_000_000,
                    "note": "Took Hilton private at cycle peak. Nearly went bankrupt in GFC. Held on, refinanced, re-IPO'd 2013. Made $14B profit. Best PE deal in history.",
                    "confidence": "confirmed",
                },
                {
                    "deal": "Logicor (European logistics)",
                    "year": 2017,
                    "price_usd": 14_000_000_000,
                    "note": "Sold to China Investment Corporation (CIC). Largest private RE deal in European history.",
                    "confidence": "confirmed",
                },
                {
                    "deal": "QTS Realty (data centers)",
                    "year": 2021,
                    "price_usd": 10_000_000_000,
                    "note": "Bet on data center growth. Now core to Blackstone's AI infrastructure thesis.",
                    "confidence": "confirmed",
                },
                {
                    "deal": "Simply Self Storage",
                    "year": 2021,
                    "price_usd": 1_200_000_000,
                    "note": "Self-storage: pandemic beneficiary thesis.",
                    "confidence": "confirmed",
                },
            ],
            "strategy": (
                "Thematic investing at massive scale. Current themes: logistics/warehouses, "
                "data centers (AI demand), rental housing, India real estate, life sciences. "
                "Exiting: traditional office (bearish). Fund structure: opportunistic (BREP), "
                "core+ (BCORE), debt (BREDS), non-traded REIT (BREIT), European (BREP Europe). "
                "Jon Gray's mantra: 'Follow the demographics and the technology.'"
            ),
            "political_connections": {
                "lobbying_spend_2024_usd": 18_500_000,
                "lobbying_confidence": "confirmed",
                "pac_contributions_2024_usd": 5_200_000,
                "pac_confidence": "estimated",
                "key_lobbying_issues": [
                    "Carried interest tax treatment (Section 1061) -- existential issue",
                    "REIT tax rules (BREIT structure)",
                    "SEC private fund adviser rules (fought vigorously)",
                    "CFIUS reviews for foreign LP capital",
                    "Housing policy (tenant protections threaten rental portfolio)",
                    "Data center permitting and power grid access",
                ],
                "revolving_door": [
                    {
                        "name": "Wayne Berman",
                        "role": "Blackstone Senior Managing Director, Head of Government Affairs",
                        "previous": "Assistant Secretary of Commerce (Bush 41)",
                        "note": "Top GOP fundraiser and power broker. Connected to every Republican administration since Reagan.",
                        "confidence": "confirmed",
                    },
                ],
                "housing_controversy": {
                    "detail": (
                        "Blackstone is the largest private landlord in the US via Invitation Homes "
                        "(spun off 2017) and other portfolio companies. UN Human Rights rapporteur "
                        "sent letter to Blackstone (2019) raising concerns about financialization "
                        "of housing. Critics allege rent increases and eviction practices. "
                        "Blackstone disputes characterization."
                    ),
                    "confidence": "confirmed",
                },
            },
            "offshore_structures": [
                {
                    "detail": "Blackstone funds are structured through Cayman and Delaware vehicles. LP capital flows through offshore feeders. Standard PE structure but scale ($340B RE) means enormous tax optimization.",
                    "confidence": "confirmed",
                },
            ],
            "systemic_risk": {
                "risk_level": "high",
                "rationale": (
                    "Single largest private RE investor globally. BREIT redemption gates showed "
                    "liquidity mismatch risk. If Blackstone RE marked down significantly, it would "
                    "cascade through pension/endowment allocations, sovereign wealth funds, and "
                    "wealth management channels. Too large to ignore."
                ),
                "confidence": "derived",
            },
        },

        # -- Brookfield Asset Management (RE) -----------------------------
        "brookfield_re": {
            "name": "Brookfield Real Estate",
            "parent": "Brookfield Asset Management (BAM) / Brookfield Corporation (BN)",
            "parent_ticker": "BAM / BN",
            "aum_usd": 270_000_000_000,
            "aum_confidence": "estimated",
            "hq": "Toronto, Canada (also NYC, London, Dubai, Mumbai, Sydney)",
            "key_personnel": [
                {
                    "name": "Bruce Flatt",
                    "title": "CEO, Brookfield Asset Management",
                    "net_worth_usd": 10_000_000_000,
                    "net_worth_confidence": "estimated",
                    "background": (
                        "Joined Brookfield 1990. Built it from a Canadian property company into "
                        "a global alternative asset manager ($1T+ AUM across RE, infrastructure, "
                        "renewables, PE, credit). Known as the 'Canadian Warren Buffett'. "
                        "Extremely private. Strategy: buy distressed hard assets in downturns."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "name": "Connor Teskey",
                    "title": "President, Brookfield Asset Management",
                    "note": "Likely successor to Flatt",
                    "confidence": "confirmed",
                },
                {
                    "name": "Brian Kingston",
                    "title": "CEO, Brookfield Property Group (former)",
                    "note": "Oversaw Brookfield's office portfolio through COVID stress period",
                    "confidence": "confirmed",
                },
            ],
            "strategy": (
                "Buy distressed, hard-asset-rich businesses. Brookfield RE is #2 globally "
                "behind Blackstone. Key difference: Brookfield owns and operates more directly "
                "(vs Blackstone's fund-management-fee model). Major positions: office (contrarian bet), "
                "retail (Brookfield Property Partners), logistics, multifamily, hospitality. "
                "Also builds data centers and infrastructure. Acquired American Tower India "
                "business 2024."
            ),
            "office_exposure": {
                "detail": (
                    "Brookfield is one of the largest office owners globally. Brookfield Property "
                    "Partners (BPY) was taken private in 2021. Some office assets in distress: "
                    "Brookfield defaulted on loans for two LA office towers (2023) and Washington DC "
                    "offices. Strategy: selectively default on non-recourse loans for underwater "
                    "assets while maintaining core portfolio."
                ),
                "selective_defaults": [
                    {"property": "Gas Company Tower + 777 Tower, Los Angeles", "year": 2023, "detail": "Defaulted on $784M loan. Handed back to lenders."},
                    {"property": "Washington DC office portfolio", "year": 2023, "detail": "Multiple assets returned to lenders."},
                ],
                "confidence": "confirmed",
            },
            "kushner_666_fifth": {
                "detail": "Brookfield's real estate arm (through Brookfield Property Partners) provided $1.1B 99-year lease on 666 Fifth Avenue retail condo (2018) to refinance Kushner Companies' troubled acquisition. QIA (Qatar) is a Brookfield LP.",
                "controversy": "Deal occurred while Kushner was senior White House adviser. GAO and media investigated potential conflicts. No enforcement action taken.",
                "confidence": "confirmed",
            },
            "political_connections": {
                "lobbying_spend_2024_usd": 8_000_000,
                "lobbying_confidence": "estimated",
                "key_lobbying_issues": [
                    "Infrastructure spending (Brookfield is major infrastructure investor)",
                    "Renewable energy tax credits (Brookfield Renewable)",
                    "Cross-border investment rules (Canadian parent, global operations)",
                    "Carried interest / fund taxation",
                ],
                "mark_carney_connection": {
                    "detail": "Mark Carney (former Governor of Bank of England and Bank of Canada) served as vice chairman of Brookfield Asset Management before becoming Prime Minister of Canada (2025). Raised conflict of interest concerns.",
                    "confidence": "confirmed",
                },
            },
            "systemic_risk": {
                "risk_level": "moderate-high",
                "rationale": "Massive office exposure at a time of structural office decline. Selective defaults signal stress. But Brookfield's diversification (infrastructure, renewables) provides buffer.",
                "confidence": "derived",
            },
        },

        # -- Starwood Capital -----------------------------------------------
        "starwood_capital": {
            "name": "Starwood Capital Group",
            "aum_usd": 115_000_000_000,
            "aum_confidence": "estimated",
            "hq": "Miami Beach, FL",
            "key_personnel": [
                {
                    "name": "Barry Sternlicht",
                    "title": "Founder, Chairman & CEO",
                    "net_worth_usd": 4_500_000_000,
                    "net_worth_confidence": "estimated",
                    "background": (
                        "Founded Starwood Capital 1991. Created Starwood Hotels & Resorts "
                        "(W Hotels, Westin, Sheraton, St. Regis) and sold to Marriott 2016 ($13B). "
                        "Outspoken critic of Fed policy and office market conditions. Runs SREIT "
                        "(non-traded REIT, similar to BREIT). Also hit with redemption gates."
                    ),
                    "public_statements": {
                        "note": "Sternlicht is unusually public for a PE executive. Frequently appears on CNBC, criticizes Fed, warns about CRE crisis.",
                        "confidence": "confirmed",
                    },
                    "confidence": "confirmed",
                },
            ],
            "sreit_redemption_issues": {
                "detail": "Starwood REIT (SREIT) also faced redemption pressure in 2022-2023, similar to BREIT. Imposed gates. Smaller scale but same structural issue.",
                "confidence": "confirmed",
            },
            "strategy": "Opportunistic and value-add across hotels, residential, office, industrial. Known for hospitality expertise (Starwood Hotels heritage). Increasingly focused on residential and logistics, reducing office.",
            "political_connections": {
                "lobbying_spend_2024_usd": 2_500_000,
                "lobbying_confidence": "estimated",
                "sternlicht_political": "Democrat-leaning but donates to both parties. Vocal critic of Biden-era interest rate policy.",
                "confidence": "estimated",
            },
            "systemic_risk": {
                "risk_level": "moderate",
                "rationale": "SREIT redemption gates plus large hospitality portfolio creates concentration risk. Smaller than Blackstone but same structural issues.",
                "confidence": "derived",
            },
        },

        # -- Irvine Company ------------------------------------------------
        "irvine_company": {
            "name": "The Irvine Company",
            "structure": "Privately held",
            "estimated_value_usd": 40_000_000_000,
            "estimated_value_confidence": "estimated",
            "hq": "Newport Beach, CA",
            "land_owned_acres": 120_000,
            "land_confidence": "estimated",
            "key_personnel": [
                {
                    "name": "Donald Bren",
                    "title": "Chairman (sole owner)",
                    "net_worth_usd": 17_000_000_000,
                    "net_worth_confidence": "estimated",
                    "age": 93,
                    "background": (
                        "Acquired Irvine Company 1983. Sole owner since 1996. Largest private "
                        "landowner in California. Controls ~1/5 of Orange County, CA. "
                        "Extremely private -- rarely photographed, almost never gives interviews. "
                        "Master-planned community of Irvine (300K+ residents) is his creation."
                    ),
                    "philanthropy": {
                        "total_donated_usd": 4_000_000_000,
                        "focus": "UC Irvine, Caltech, conservation. Bren Hall at UCI.",
                        "confidence": "estimated",
                    },
                    "confidence": "confirmed",
                },
            ],
            "portfolio": {
                "residential": "Master-planned communities across Irvine, Newport Beach, Tustin, Woodbury",
                "office": "500+ office buildings in Orange County, Silicon Valley, San Diego",
                "retail": "Irvine Spectrum Center, Fashion Island, The Market Place",
                "apartments": "60,000+ apartment units (Irvine Company Apartment Communities)",
                "confidence": "estimated",
            },
            "strategy": (
                "Develop, own, and never sell. Bren's philosophy: master-plan communities, "
                "build the infrastructure, retain ownership of commercial/retail/multifamily "
                "forever. Generates massive recurring cash flow from rents. Has never taken "
                "the company public. Land bank is irreplaceable."
            ),
            "succession_risk": {
                "detail": "Bren is 93, no public succession plan. Company reportedly has internal governance structure. Bren has donated heavily to charity (some assets may go to foundation). Private ownership means transition will be opaque.",
                "confidence": "rumored",
            },
            "political_connections": {
                "lobbying": "Extensive local lobbying in Orange County for zoning, water rights, transportation infrastructure. Lower federal profile than Blackstone/Brookfield.",
                "donations": "Bren has donated to both parties. Major donor to environmental conservation causes.",
                "confidence": "estimated",
            },
            "systemic_risk": {
                "risk_level": "low",
                "rationale": "Zero debt (reportedly), no outside investors, private = no redemption risk. Succession is the risk, not financial distress.",
                "confidence": "inferred",
            },
        },
    },

    # ======================================================================
    # COMMERCIAL REAL ESTATE CRISIS
    # ======================================================================

    "cre_crisis": {
        "overview": (
            "The post-COVID commercial real estate crisis is the most significant RE dislocation "
            "since the 2008 GFC. Work-from-home has structurally reduced office demand. "
            "Regional banks are disproportionately exposed to CRE loans. CMBS delinquency rates "
            "are rising. The crisis is slow-moving (long leases delay recognition) but the "
            "magnitude is enormous: $1.5T in CRE loans mature 2024-2026."
        ),
        "overview_confidence": "confirmed",

        "office_vacancy_by_city": {
            "san_francisco": {"vacancy_pct": 37.0, "note": "Worst in US. Tech WFH + layoffs devastated demand.", "confidence": "confirmed"},
            "austin": {"vacancy_pct": 28.0, "note": "Massive oversupply from 2021-2022 building boom.", "confidence": "estimated"},
            "houston": {"vacancy_pct": 25.0, "note": "Chronic oversupply. Energy sector office footprint shrinking.", "confidence": "estimated"},
            "chicago": {"vacancy_pct": 24.0, "note": "Loop office buildings struggling. Suburban office even worse.", "confidence": "estimated"},
            "los_angeles": {"vacancy_pct": 23.0, "note": "Entertainment industry WFH + tech pullback.", "confidence": "estimated"},
            "dallas": {"vacancy_pct": 22.0, "note": "New supply delivered into weakening demand.", "confidence": "estimated"},
            "washington_dc": {"vacancy_pct": 22.0, "note": "Federal WFH policies reduced government office demand.", "confidence": "estimated"},
            "new_york": {"vacancy_pct": 22.0, "note": "Class A holding better (sub-15%). Class B/C at 30%+.", "confidence": "estimated"},
            "denver": {"vacancy_pct": 21.0, "confidence": "estimated"},
            "boston": {"vacancy_pct": 18.0, "note": "Life sciences demand providing partial offset.", "confidence": "estimated"},
            "miami": {"vacancy_pct": 14.0, "note": "Benefiting from NY/CA migration. Tightest in US.", "confidence": "estimated"},
            "london": {"vacancy_pct": 10.0, "note": "City of London and West End bifurcated. New prime well-leased; secondary struggling.", "confidence": "estimated"},
            "tokyo": {"vacancy_pct": 5.5, "note": "Lower WFH adoption in Japan. Vacancy rising but still low globally.", "confidence": "estimated"},
            "singapore": {"vacancy_pct": 6.0, "note": "Tight supply, Asia-Pacific hub status. Grade A doing well.", "confidence": "estimated"},
        },

        "cmbs_delinquency": {
            "overall_rate_pct": 6.5,
            "office_rate_pct": 11.0,
            "retail_rate_pct": 7.5,
            "hotel_rate_pct": 5.0,
            "industrial_rate_pct": 0.5,
            "multifamily_rate_pct": 3.5,
            "data_source": "Trepp",
            "as_of": "2025-Q4",
            "trend": "Office CMBS delinquency accelerating. Industrial near zero. Multifamily rising in Sun Belt (oversupply).",
            "confidence": "estimated",
        },

        "cre_loan_maturity_wall": {
            "total_maturing_2024_2026_usd": 1_500_000_000_000,
            "detail": (
                "~$1.5T in CRE loans mature 2024-2026. Many originated at 3-4% rates, now must "
                "refinance at 6-8%. Properties that have lost value may not qualify for same LTV. "
                "'Extend and pretend' is prevalent: banks modifying loans to defer recognition of losses."
            ),
            "confidence": "estimated",
        },

        "regional_bank_cre_exposure": [
            {
                "bank": "New York Community Bancorp (NYCB)",
                "ticker": "NYCB",
                "cre_to_total_loans_pct": 57,
                "detail": "Inherited Signature Bank CRE portfolio (FDIC deal). Stock crashed 70% in early 2024 after surprise loss + dividend cut. Received $1B rescue from consortium led by former Treasury Secretary Steven Mnuchin (Liberty Strategic Capital).",
                "mnuchin_rescue": {
                    "amount_usd": 1_000_000_000,
                    "date": "2024-03",
                    "led_by": "Steven Mnuchin (Liberty Strategic Capital)",
                    "other_investors": ["Hudson Bay Capital", "Reverence Capital", "Citadel"],
                    "note": "Mnuchin (Trump Treasury Secretary) acquiring distressed bank CRE exposure. Pattern: former officials monetizing crisis they helped create.",
                    "confidence": "confirmed",
                },
                "confidence": "confirmed",
            },
            {
                "bank": "Valley National Bancorp",
                "ticker": "VLY",
                "cre_to_total_loans_pct": 48,
                "detail": "NJ-based bank with heavy CRE concentration. Under regulatory pressure to diversify.",
                "confidence": "estimated",
            },
            {
                "bank": "OceanFirst Financial",
                "ticker": "OCFC",
                "cre_to_total_loans_pct": 45,
                "detail": "NJ/NY metro CRE lender. Office exposure is concern.",
                "confidence": "estimated",
            },
            {
                "bank": "Columbia Banking System",
                "ticker": "COLB",
                "cre_to_total_loans_pct": 42,
                "detail": "Pacific Northwest CRE concentration.",
                "confidence": "estimated",
            },
            {
                "bank": "Axos Financial",
                "ticker": "AX",
                "cre_to_total_loans_pct": 40,
                "detail": "Online bank with significant CRE book. Hindenburg short report (2024) alleged questionable CRE lending practices.",
                "confidence": "estimated",
            },
        ],

        "extend_and_pretend": {
            "detail": (
                "Banks are modifying CRE loans rather than recognizing losses. Federal regulators "
                "(OCC, FDIC) have issued guidance allowing 'prudent loan workouts' which critics "
                "call regulatory forbearance. The Japanese 'zombie loan' parallel is apt: "
                "delay recognition, extend maturity, pray for rate cuts."
            ),
            "estimated_zombie_cre_loans_usd": 500_000_000_000,
            "confidence": "estimated",
        },

        "wfh_impact": {
            "office_utilization_pct": 50,
            "detail": "Kastle Systems back-to-office barometer shows ~50% average utilization across top 10 US metros. Tuesday-Thursday highest (60%+), Monday/Friday lowest (30%). Structural shift, not temporary.",
            "cbre_forecast": "CBRE projects US office vacancy won't peak until 2026 and won't recover to pre-COVID levels until 2030+.",
            "greenstreet_cppi": {
                "office_price_decline_from_peak_pct": -35,
                "note": "Green Street Commercial Property Price Index shows office values down ~35% from 2022 peak. Some Class B/C assets down 50-70%.",
                "confidence": "estimated",
            },
            "conversion_trend": {
                "detail": "Office-to-residential conversions accelerating. NYC, Chicago, DC, Calgary leading. But conversion is expensive ($200-400/sqft) and structurally difficult (floorplate depth, plumbing).",
                "confidence": "confirmed",
            },
            "confidence": "confirmed",
        },

        "systemic_risk_assessment": {
            "risk_level": "high",
            "detail": (
                "CRE crisis is the most likely trigger for a regional banking crisis. $1.5T maturity "
                "wall + 35% office value decline + regional bank concentration = classic credit cycle "
                "dynamics. However: (1) large banks have limited direct CRE exposure, (2) CMBS losses "
                "are distributed across investors, (3) Fed rate cuts would provide relief. "
                "Scenario: 2-3 more regional bank failures possible if rates stay elevated + office "
                "vacancies don't improve. Not 2008 magnitude but could trigger FDIC fund stress."
            ),
            "fed_sloos_data": "Senior Loan Officer Survey shows banks have tightened CRE lending standards to 2008 levels. New CRE loan origination down 50%+ from peak.",
            "confidence": "derived",
        },
    },

    # ======================================================================
    # CROSS-NETWORK CONNECTIONS
    # ======================================================================

    "cross_network_connections": [
        {
            "connection": "Blackstone <-> BREIT <-> UC system <-> pension allocations",
            "detail": "Blackstone RE funds are funded by pensions, endowments, SWFs. BREIT is the retail channel. UC investment in BREIT links sovereign/public capital to private RE liquidity risk.",
            "confidence": "confirmed",
        },
        {
            "connection": "Vornado <-> Kushner <-> Brookfield <-> Qatar (QIA)",
            "detail": "666 Fifth Avenue links Vornado (Roth), Kushner Companies, Brookfield ($1.1B lease), and QIA (Brookfield LP). Political connections to Trump White House.",
            "confidence": "confirmed",
        },
        {
            "connection": "UAE royals (ADIA/Mubadala) <-> Equinix xScale <-> Blackstone data centers",
            "detail": "Sovereign wealth funds from UAE are co-investors in both Equinix xScale JVs and Blackstone data center platforms. Sovereign capital backing AI infrastructure buildout.",
            "confidence": "confirmed",
        },
        {
            "connection": "Singapore SWF (Temasek/GIC) <-> CapitaLand/Mapletree <-> global RE allocations",
            "detail": "Singapore sovereign capital flows through captive RE platforms (CapitaLand, Mapletree) and direct GIC investments in US/European RE. $400B+ in sovereign capital with significant RE allocation.",
            "confidence": "confirmed",
        },
        {
            "connection": "Japan (BOJ) <-> J-REITs <-> Mitsui/Mitsubishi keiretsu",
            "detail": "BOJ bought J-REIT ETFs as monetary policy. Keiretsu cross-shareholdings mean Japanese RE is intertwined with banking and trading company balance sheets.",
            "confidence": "confirmed",
        },
        {
            "connection": "Chinese developer crisis <-> global commodity demand <-> EM bond markets",
            "detail": "Chinese property ($5T developer debt) collapse reduces global demand for iron ore (Australia), copper (Chile/Peru), and hit offshore EM bond funds (PIMCO, BlackRock, Ashmore).",
            "confidence": "derived",
        },
        {
            "connection": "Regional banks <-> CRE loans <-> FDIC <-> deposit insurance fund",
            "detail": "CRE concentration in regional banks threatens FDIC fund. SVB/Signature/First Republic failures already depleted fund. 2-3 more failures would stress the system.",
            "confidence": "derived",
        },
        {
            "connection": "Schwarzman <-> Trump <-> Mnuchin <-> NYCB rescue <-> CRE distressed investing",
            "detail": "GOP financial ecosystem: Schwarzman (Blackstone) donates to Trump. Mnuchin (Trump Treasury) rescues NYCB with CRE exposure. Pattern of political insiders profiting from crises they influenced.",
            "confidence": "inferred",
        },
        {
            "connection": "Prologis data center pivot <-> Equinix <-> hyperscaler demand <-> AI capex cycle",
            "detail": "Prologis leveraging industrial land for data centers, competing/complementing Equinix. Both serving AI infrastructure buildout. $100B+/year hyperscaler capex flowing to RE.",
            "confidence": "confirmed",
        },
        {
            "connection": "Brookfield <-> Mark Carney (Canada PM) <-> infrastructure policy",
            "detail": "Carney was Brookfield vice chairman before becoming PM. Brookfield benefits from Canadian infrastructure and climate policy. Revolving door between sovereign policy and private capital.",
            "confidence": "confirmed",
        },
    ],
}
