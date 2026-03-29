"""
GRID Intelligence — Global Energy Sector Power Network Map.

Static intelligence dossier on the global energy power structure:
oil majors, OPEC+ sovereign producers, commodity traders, and
renewable energy leaders.

All data sourced from public filings (10-K, DEF 14A, Form 4),
OpenSecrets, OPEC reports, company annual reports, ICIJ leaks,
and credible journalism (FT, Reuters, Bloomberg).

Confidence labels per GRID convention:
    confirmed  — directly from SEC filings, annual reports, or government databases
    derived    — calculated from confirmed data
    estimated  — credible third-party estimate (OpenSecrets, SIPRI, Reuters, etc.)
    rumored    — reported in media but unverified
    inferred   — pattern-detected by GRID analysis

Data vintage: public information through early 2025.
Update frequency: refresh quarterly after earnings season + OPEC meetings.

Key entry points:
    get_energy_network()               — full network dict
    get_entity(ticker_or_id)           — single entity dossier
    get_opec_dynamics()                — OPEC+ power balance + compliance
    get_trading_houses()               — commodity trader profiles
    get_energy_lobbying_summary()      — aggregated lobbying + PAC spend
    get_geopolitical_risk_signals()    — sanctions, arbitration, license risks
    get_renewable_transition_signals() — who is pivoting, who is retreating
"""

from __future__ import annotations

from typing import Any


def get_energy_network() -> dict[str, Any]:
    """Return the full energy sector intelligence network."""
    return ENERGY_NETWORK


def get_entity(ticker_or_id: str) -> dict[str, Any] | None:
    """Return dossier for a single entity by ticker or ID."""
    for section in ("oil_majors", "opec_plus", "energy_traders", "renewables"):
        entities = ENERGY_NETWORK.get(section, {})
        if ticker_or_id in entities:
            return entities[ticker_or_id]
    return None


def get_opec_dynamics() -> dict[str, Any]:
    """Return OPEC+ power balance and compliance data."""
    return ENERGY_NETWORK.get("opec_plus_dynamics", {})


def get_trading_houses() -> list[dict[str, Any]]:
    """Return all commodity trading house profiles."""
    return list(ENERGY_NETWORK.get("energy_traders", {}).values())


def get_energy_lobbying_summary() -> dict[str, Any]:
    """Aggregate lobbying + PAC spend across all energy entities."""
    total_lobbying = 0.0
    total_pac = 0.0
    for section in ("oil_majors", "renewables"):
        for _id, entity in ENERGY_NETWORK.get(section, {}).items():
            total_lobbying += entity.get("lobbying", {}).get("annual_spend_usd", 0)
            total_pac += entity.get("pac_contributions", {}).get("total_2024_cycle_usd", 0)
    return {
        "total_annual_lobbying_usd": total_lobbying,
        "total_pac_2024_cycle_usd": total_pac,
        "confidence": "estimated",
        "source": "OpenSecrets aggregation",
    }


def get_geopolitical_risk_signals() -> list[dict[str, Any]]:
    """Extract active geopolitical risk signals from the network."""
    signals = []
    for section in ("oil_majors", "opec_plus"):
        for _id, entity in ENERGY_NETWORK.get(section, {}).items():
            for risk in entity.get("geopolitical_risks", []):
                risk_copy = dict(risk)
                risk_copy["entity_id"] = _id
                risk_copy["entity_name"] = entity.get("name", _id)
                signals.append(risk_copy)
    return signals


def get_renewable_transition_signals() -> list[dict[str, Any]]:
    """Who is pivoting toward renewables, who is retreating."""
    signals = []
    for _id, entity in ENERGY_NETWORK.get("oil_majors", {}).items():
        transition = entity.get("energy_transition", {})
        if transition:
            signals.append({
                "entity_id": _id,
                "entity_name": entity["name"],
                **transition,
            })
    return signals


# ══════════════════════════════════════════════════════════════════════════
# STATIC INTELLIGENCE DATA
# ══════════════════════════════════════════════════════════════════════════

ENERGY_NETWORK: dict[str, Any] = {
    "meta": {
        "report_type": "global_energy_power_network",
        "version": "1.0.0",
        "data_vintage": "2025-Q1",
        "refresh_cadence": "quarterly",
        "classification": "OSINT",
        "sources": [
            "SEC EDGAR (10-K, DEF 14A, Form 4, 13F)",
            "OpenSecrets.org",
            "OPEC Monthly Oil Market Report",
            "IEA World Energy Outlook",
            "Company Annual Reports / Investor Presentations",
            "Reuters / Bloomberg / Financial Times",
            "ICIJ Offshore Leaks Database",
            "Senate Lobbying Disclosure Act filings",
            "US Treasury OFAC sanctions lists",
            "EU sanctions regulations",
        ],
    },

    # ══════════════════════════════════════════════════════════════════
    # OIL MAJORS
    # ══════════════════════════════════════════════════════════════════

    "oil_majors": {

        # ── EXXONMOBIL (XOM) ────────────────────────────────────────
        "XOM": {
            "name": "Exxon Mobil Corporation",
            "ticker": "XOM",
            "exchange": "NYSE",
            "sector": "Integrated Oil & Gas",
            "market_cap_usd": 510_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 344_600_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2024_usd": 33_700_000_000,
            "net_income_confidence": "confirmed",
            "employees": 62_000,
            "hq": "Spring, TX (formerly Irving, TX)",

            "ceo": {
                "name": "Darren W. Woods",
                "title": "Chairman & CEO",
                "appointed": "2017-01-01",
                "total_compensation_2023_usd": 36_600_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2024",
                "background": (
                    "Career ExxonMobil executive since 1992. Electrical engineering "
                    "degree from Texas A&M, MBA from Northwestern Kellogg. Rose "
                    "through refining and chemicals divisions. Known for doubling "
                    "down on fossil fuels while peers explored transition."
                ),
                "confidence": "confirmed",
            },

            "key_people": [
                {
                    "name": "Kathryn A. Mikells",
                    "title": "Senior VP & CFO",
                    "appointed": "2021-08-09",
                    "confidence": "confirmed",
                },
                {
                    "name": "Neil A. Chapman",
                    "title": "Senior VP",
                    "role_detail": "Oversees upstream operations",
                    "confidence": "confirmed",
                },
                {
                    "name": "Jack P. Williams Jr.",
                    "title": "Senior VP",
                    "role_detail": "Downstream and chemicals",
                    "confidence": "confirmed",
                },
            ],

            "major_acquisitions": [
                {
                    "target": "Pioneer Natural Resources",
                    "ticker": "PXD",
                    "value_usd": 64_500_000_000,
                    "announced": "2023-10-11",
                    "closed": "2024-05-03",
                    "type": "all_stock",
                    "strategic_rationale": (
                        "Transforms XOM into dominant Permian Basin operator. "
                        "Added ~700K net acres, doubling Permian production to "
                        "~1.3M boe/d. Pioneer CEO Scott Sheffield barred from "
                        "XOM board by FTC over alleged OPEC collusion."
                    ),
                    "ftc_conditions": (
                        "FTC approved with condition that Scott Sheffield be "
                        "permanently barred from ExxonMobil board due to evidence "
                        "he coordinated with OPEC officials to limit production. "
                        "Sheffield denies allegations."
                    ),
                    "confidence": "confirmed",
                    "source": "SEC filings, FTC consent order July 2024",
                },
                {
                    "target": "Denbury Inc.",
                    "value_usd": 4_900_000_000,
                    "closed": "2023-11",
                    "strategic_rationale": "Carbon capture pipeline network, largest in US",
                    "confidence": "confirmed",
                },
            ],

            "key_assets": [
                {
                    "asset": "Guyana (Stabroek Block)",
                    "type": "upstream",
                    "interest_pct": 45,
                    "operator": True,
                    "partners": ["Hess (30%)", "CNOOC (25%)"],
                    "production_boed": 640_000,
                    "discovered_resources_boe": 11_000_000_000,
                    "notes": (
                        "Transformational discovery since 2015. 6 FPSOs sanctioned. "
                        "Among lowest-cost barrels globally at ~$35/bbl breakeven. "
                        "Subject to Chevron-Hess arbitration that could reshape "
                        "ownership structure."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "asset": "Permian Basin (incl. Pioneer)",
                    "type": "upstream",
                    "production_boed": 1_300_000,
                    "notes": "Largest Permian operator post-Pioneer",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Beaumont Refinery Expansion",
                    "type": "downstream",
                    "capacity_bpd": 620_000,
                    "notes": "Largest refinery in US after 250K bpd expansion completed 2023",
                    "confidence": "confirmed",
                },
                {
                    "asset": "LNG Portfolio",
                    "type": "midstream",
                    "notes": "Golden Pass LNG (70% XOM, 30% QatarEnergy) under construction. Mozambique LNG delayed by insurgency.",
                    "confidence": "confirmed",
                },
            ],

            "climate_history": {
                "summary": (
                    "Internal scientists documented climate change risks as early as "
                    "1977-1982 (confirmed by investigative journalism, Exxon internal "
                    "memos). Company subsequently funded climate denial organizations "
                    "for decades. Facing multiple state AG lawsuits. Lost landmark "
                    "Engine No. 1 proxy fight in 2021 (3 dissident directors elected). "
                    "Now positions carbon capture (CCS) as primary climate strategy "
                    "rather than emissions reduction."
                ),
                "key_events": [
                    {
                        "year": 1982,
                        "event": "Internal memo accurately predicted CO2 levels and warming",
                        "confidence": "confirmed",
                        "source": "InsideClimate News investigation 2015",
                    },
                    {
                        "year": "1998-2007",
                        "event": "Funded American Petroleum Institute, Heartland Institute, George C. Marshall Institute climate denial efforts",
                        "amount_estimated_usd": 39_000_000,
                        "confidence": "estimated",
                        "source": "Union of Concerned Scientists / Greenpeace",
                    },
                    {
                        "year": 2021,
                        "event": "Engine No. 1 hedge fund wins 3 board seats in historic proxy fight",
                        "confidence": "confirmed",
                    },
                    {
                        "year": 2024,
                        "event": "Sued shareholders who submitted climate resolutions, chilling effect on ESG activism",
                        "confidence": "confirmed",
                    },
                ],
                "active_litigation": [
                    "Massachusetts AG v. ExxonMobil (consumer fraud)",
                    "New York AG v. ExxonMobil (securities fraud - dismissed 2019)",
                    "Multiple California municipal lawsuits (public nuisance)",
                    "Ramirez v. ExxonMobil (greenwashing, TX federal court)",
                ],
                "confidence": "confirmed",
            },

            "energy_transition": {
                "strategy": "fossil_fuel_maximalist_with_ccs",
                "capex_low_carbon_pct": 5,
                "capex_low_carbon_annual_usd": 1_700_000_000,
                "focus_areas": ["Carbon capture (CCS/CCUS)", "Hydrogen (blue)", "Biofuels"],
                "renewables_investment": "minimal",
                "net_zero_target": "Scope 1+2 by 2050, no Scope 3 target",
                "confidence": "confirmed",
                "trading_signal": (
                    "XOM is the anti-transition play. Benefits from prolonged fossil "
                    "fuel demand. Vulnerable to rapid decarbonization scenarios and "
                    "litigation risk."
                ),
            },

            "lobbying": {
                "annual_spend_usd": 9_900_000,
                "year": 2023,
                "registered_lobbyists": 56,
                "key_issues": [
                    "Carbon capture tax credits (45Q expansion)",
                    "LNG export approvals",
                    "Permian Basin permitting",
                    "EPA methane rule pushback",
                ],
                "confidence": "confirmed",
                "source": "Senate LDA filings via OpenSecrets",
            },

            "pac_contributions": {
                "pac_name": "Exxon Mobil Corporation Political Action Committee (ExxonMobil PAC)",
                "total_2024_cycle_usd": 1_700_000,
                "split_pct": {"republican": 73, "democrat": 27},
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "offshore_structures": {
                "known_subsidiaries_in_tax_havens": [
                    {"jurisdiction": "Bahamas", "entity": "ExxonMobil subsidiaries", "purpose": "Shipping/insurance", "confidence": "confirmed"},
                    {"jurisdiction": "Netherlands", "entity": "ExxonMobil Holding Company", "purpose": "European holding structure", "confidence": "confirmed"},
                    {"jurisdiction": "Delaware", "entity": "Multiple LLCs", "purpose": "Domestic IP and asset holding", "confidence": "confirmed"},
                ],
                "effective_tax_rate_2023_pct": 24.5,
                "tax_rate_confidence": "confirmed",
                "tax_rate_source": "10-K 2023",
            },

            "geopolitical_risks": [
                {
                    "risk": "Guyana sovereignty dispute with Venezuela",
                    "severity": "medium",
                    "description": "Venezuela claims Essequibo region (western 2/3 of Guyana). Maduro held referendum in Dec 2023. US military support for Guyana reduces escalation risk.",
                    "confidence": "confirmed",
                },
                {
                    "risk": "Chevron-Hess arbitration over Guyana preemptive rights",
                    "severity": "high",
                    "description": "If Chevron wins ICC arbitration, XOM's Stabroek Block partnership could be restructured. Decision expected 2025.",
                    "confidence": "confirmed",
                },
                {
                    "risk": "Mozambique LNG insurgency",
                    "severity": "medium",
                    "description": "Islamist insurgency in Cabo Delgado delayed $30B LNG project. Partial resumption underway.",
                    "confidence": "confirmed",
                },
            ],

            "insider_trading_pattern": {
                "net_pattern_12mo": "net_selling",
                "confidence": "confirmed",
                "source": "SEC Form 4 filings",
            },

            "trading_signals": [
                {
                    "signal": "Pioneer integration synergies",
                    "direction": "bullish",
                    "timeframe": "12-24 months",
                    "description": "Cost synergies target $2B+/year by 2026. Permian dominance creates pricing power.",
                    "confidence": "derived",
                },
                {
                    "signal": "Guyana arbitration outcome",
                    "direction": "uncertain",
                    "timeframe": "2025",
                    "description": "Chevron winning preemptive rights could force XOM to accept new Guyana partner dynamics.",
                    "confidence": "inferred",
                },
                {
                    "signal": "Litigation overhang",
                    "direction": "bearish",
                    "timeframe": "ongoing",
                    "description": "Climate litigation could result in multi-billion settlements. No material provisions yet.",
                    "confidence": "inferred",
                },
            ],
        },

        # ── CHEVRON (CVX) ───────────────────────────────────────────
        "CVX": {
            "name": "Chevron Corporation",
            "ticker": "CVX",
            "exchange": "NYSE",
            "sector": "Integrated Oil & Gas",
            "market_cap_usd": 290_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 196_900_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2024_usd": 18_300_000_000,
            "net_income_confidence": "confirmed",
            "employees": 43_800,
            "hq": "San Ramon, CA (relocating HQ to Houston, TX)",

            "ceo": {
                "name": "Michael K. Wirth",
                "title": "Chairman & CEO",
                "appointed": "2018-02-01",
                "total_compensation_2023_usd": 29_200_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2024",
                "background": (
                    "Career Chevron executive since 1982 (chemical engineering, "
                    "U of Wisconsin). Rose through downstream/chemicals. Known for "
                    "capital discipline. Navigated Venezuela license restoration."
                ),
                "confidence": "confirmed",
            },

            "major_acquisitions": [
                {
                    "target": "Hess Corporation",
                    "ticker": "HES",
                    "value_usd": 60_000_000_000,
                    "announced": "2023-10-23",
                    "status": "pending_arbitration",
                    "type": "all_stock",
                    "strategic_rationale": (
                        "Primary target: Hess's 30% stake in Guyana Stabroek Block. "
                        "Would give Chevron a seat at the world's most important new "
                        "oil province. Deal blocked by ExxonMobil/CNOOC preemptive "
                        "rights claim — ICC arbitration underway."
                    ),
                    "arbitration_detail": (
                        "ExxonMobil filed ICC arbitration claiming preemptive rights "
                        "over Hess's Stabroek stake under the joint operating agreement. "
                        "CNOOC joined the claim. Hearing expected mid-2025. If ExxonMobil "
                        "wins, Chevron may not get Guyana access — deal could collapse "
                        "or be restructured. Single most important M&A risk in energy."
                    ),
                    "confidence": "confirmed",
                    "source": "SEC filings, ICC arbitration public records",
                },
                {
                    "target": "PDC Energy",
                    "value_usd": 7_600_000_000,
                    "closed": "2023-08",
                    "strategic_rationale": "DJ Basin (Colorado) acreage consolidation",
                    "confidence": "confirmed",
                },
            ],

            "venezuela_license": {
                "summary": (
                    "Chevron holds OFAC General License 41 (expanded Oct 2022, "
                    "modified 2023-2024) allowing limited oil operations in Venezuela "
                    "via joint venture with PDVSA. Chevron is the only US major with "
                    "active Venezuela operations. License renewal is politically "
                    "contingent — tied to Maduro regime behavior on elections."
                ),
                "license_type": "OFAC General License 41",
                "partner": "PDVSA (state oil company)",
                "production_boed": 200_000,
                "political_sensitivity": "extreme",
                "notes": (
                    "License was briefly expanded then restricted based on Maduro "
                    "election commitments. Could be revoked at any time. Chevron "
                    "cannot expand operations, only maintain existing JVs."
                ),
                "confidence": "confirmed",
                "source": "US Treasury OFAC, Chevron 10-K",
            },

            "key_assets": [
                {
                    "asset": "Permian Basin",
                    "type": "upstream",
                    "production_boed": 860_000,
                    "notes": "#2 Permian producer after XOM. Core of US growth strategy.",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Tengizchevroil (Kazakhstan)",
                    "type": "upstream",
                    "interest_pct": 50,
                    "production_boed": 600_000,
                    "notes": (
                        "Massive TCO expansion (WPMP/FGP) cost $48.5B vs $37B budget. "
                        "Near completion. Export via CPC pipeline through Russia "
                        "creates sanctions adjacency risk."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "asset": "Australia LNG (Gorgon, Wheatstone)",
                    "type": "upstream/LNG",
                    "capacity_mtpa": 24.5,
                    "notes": "Among largest LNG projects globally. Gorgon CO2 injection underperforming.",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Gulf of Mexico deepwater",
                    "type": "upstream",
                    "production_boed": 250_000,
                    "confidence": "confirmed",
                },
            ],

            "energy_transition": {
                "strategy": "pragmatic_traditional",
                "capex_low_carbon_pct": 7,
                "focus_areas": ["Hydrogen", "Carbon capture", "Renewable fuels (renewdiesel)"],
                "renewable_fuels_capacity": "El Segundo refinery conversion to 80K bpd renewdiesel",
                "net_zero_target": "Scope 1+2 by 2050, portfolio carbon intensity reduction",
                "confidence": "confirmed",
            },

            "lobbying": {
                "annual_spend_usd": 8_800_000,
                "year": 2023,
                "registered_lobbyists": 48,
                "confidence": "confirmed",
                "source": "Senate LDA filings via OpenSecrets",
            },

            "pac_contributions": {
                "pac_name": "Chevron Employees Political Action Committee",
                "total_2024_cycle_usd": 1_500_000,
                "split_pct": {"republican": 70, "democrat": 30},
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "geopolitical_risks": [
                {
                    "risk": "Hess/Guyana arbitration failure",
                    "severity": "critical",
                    "description": "If CVX loses ICC arbitration, entire $60B Hess deal logic collapses. Would need to find alternative growth assets.",
                    "confidence": "confirmed",
                },
                {
                    "risk": "Venezuela license revocation",
                    "severity": "medium",
                    "description": "OFAC license tied to political conditions. Revocation would strand Venezuela JV investments.",
                    "confidence": "confirmed",
                },
                {
                    "risk": "Kazakhstan/Russia transit risk",
                    "severity": "medium",
                    "description": "CPC pipeline transits Russia. Periodic disruptions (storms, 'maintenance'). Sanctions creep could affect transit.",
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "Hess arbitration binary outcome",
                    "direction": "binary",
                    "timeframe": "2025",
                    "description": "Win: unlocks Guyana growth, validating $60B deal. Lose: growth strategy in question, potential deal collapse.",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Tengiz completion inflection",
                    "direction": "bullish",
                    "timeframe": "2025-2026",
                    "description": "Once WPMP/FGP ramp completes, capex drops and free cash flow surges. Historically delayed project nearing end.",
                    "confidence": "derived",
                },
            ],
        },

        # ── SHELL (SHEL) ────────────────────────────────────────────
        "SHEL": {
            "name": "Shell plc",
            "ticker": "SHEL",
            "exchange": "NYSE / LSE",
            "sector": "Integrated Oil & Gas",
            "market_cap_usd": 230_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 286_000_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2024_usd": 19_400_000_000,
            "net_income_confidence": "confirmed",
            "employees": 86_000,
            "hq": "London, UK (incorporated in UK since 2022 simplification)",

            "ceo": {
                "name": "Wael Sawan",
                "title": "CEO",
                "appointed": "2023-01-01",
                "nationality": "Lebanese-Canadian",
                "total_compensation_2023_usd": 10_400_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "Shell Annual Report 2023",
                "background": (
                    "Joined Shell 1997. Led Integrated Gas & Renewables, then "
                    "Upstream. Engineering degree (McGill), MBA (Harvard). "
                    "Pivoted Shell back toward oil/gas profitability, scaling back "
                    "renewables ambitions set by predecessor Ben van Beurden."
                ),
                "confidence": "confirmed",
            },

            "energy_transition": {
                "strategy": "retreat_from_renewables",
                "capex_low_carbon_pct": 10,
                "capex_low_carbon_annual_usd": 3_500_000_000,
                "key_retreat_actions": [
                    "Cut absolute emissions target from 30% to 15-20% by 2030",
                    "Exited European offshore wind bids (citing poor returns)",
                    "Sold UK home energy retail business",
                    "Reduced EV charging ambitions",
                    "Refocused on LNG and oil as 'core performance' businesses",
                ],
                "net_zero_target": "Net zero by 2050 (weakened interim targets)",
                "dutch_court_ruling": (
                    "2021 Hague District Court ordered Shell to cut emissions 45% "
                    "by 2030 vs 2019 (incl. Scope 3). Shell appealing. Moved HQ "
                    "from Netherlands to UK partly to reduce legal exposure. "
                    "Appeal decision expected 2024-2025."
                ),
                "confidence": "confirmed",
                "trading_signal": (
                    "Sawan's strategy pivot signals Shell is re-rating as oil/gas "
                    "company, not energy transition leader. Narrows ESG premium "
                    "but improves near-term cash flow."
                ),
            },

            "key_assets": [
                {
                    "asset": "Global LNG portfolio",
                    "type": "LNG",
                    "capacity_mtpa": 67,
                    "notes": (
                        "Largest LNG trader in the world. Operates Queensland Curtis, "
                        "Prelude FLNG, interests in Nigeria LNG, Sakhalin II (divesting). "
                        "LNG trading is Shell's crown jewel — commodity trading arm "
                        "generated $4B+ profits in 2022 energy crisis."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "asset": "Deepwater Gulf of Mexico",
                    "type": "upstream",
                    "production_boed": 350_000,
                    "confidence": "confirmed",
                },
                {
                    "asset": "Nigeria (SPDC JV)",
                    "type": "upstream",
                    "notes": "Divesting onshore Niger Delta assets. Environmental liability enormous.",
                    "confidence": "confirmed",
                },
            ],

            "lobbying": {
                "annual_spend_usd": 7_300_000,
                "year": 2023,
                "jurisdiction": "US (also significant EU/UK lobbying)",
                "confidence": "estimated",
                "source": "OpenSecrets + InfluenceMap estimates",
            },

            "controversies": [
                {
                    "issue": "Niger Delta environmental devastation",
                    "description": "Decades of oil spills in Nigeria. Multiple lawsuits including UK Supreme Court (Okpabi v Shell). Ongoing cleanup liabilities.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Sakhalin II divestment under duress",
                    "description": "Russian government seized effective control of Sakhalin Energy in 2022. Shell wrote down ~$5B. 'Sale' to Russian entities at fraction of value.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Dutch court climate ruling",
                    "description": "Ordered 45% emissions cut by 2030. Shell relocated HQ to UK. Appeal pending.",
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "LNG supercycle beneficiary",
                    "direction": "bullish",
                    "timeframe": "2024-2028",
                    "description": "Shell's unmatched LNG trading portfolio benefits from European/Asian gas demand and price volatility.",
                    "confidence": "derived",
                },
                {
                    "signal": "UK listing discount",
                    "direction": "catalyst_watch",
                    "timeframe": "2025",
                    "description": "Shell trades at persistent PE discount to US peers. Potential relisting to US would close gap. Sawan has not ruled it out.",
                    "confidence": "inferred",
                },
            ],
        },

        # ── BP (BP) ─────────────────────────────────────────────────
        "BP": {
            "name": "BP plc",
            "ticker": "BP",
            "exchange": "NYSE / LSE",
            "sector": "Integrated Oil & Gas",
            "market_cap_usd": 96_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 199_800_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2024_usd": 6_200_000_000,
            "net_income_confidence": "confirmed",
            "employees": 87_800,
            "hq": "London, UK",

            "ceo": {
                "name": "Murray Auchincloss",
                "title": "CEO",
                "appointed": "2024-01-01",
                "nationality": "Canadian",
                "total_compensation_2023_usd": 7_800_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "BP Annual Report 2023",
                "background": (
                    "CFO under predecessor Bernard Looney (who resigned over "
                    "undisclosed relationships). Chartered accountant. Career "
                    "BP finance executive. Pivoting strategy back toward oil "
                    "and gas after Looney's ambitious but underperforming renewables push."
                ),
                "predecessor_scandal": (
                    "Bernard Looney resigned Sept 2023 after admitting he had not "
                    "been fully transparent about personal relationships with "
                    "colleagues. Looney had been the architect of BP's 2020 'net zero' "
                    "strategy. His departure accelerated the return to oil focus."
                ),
                "confidence": "confirmed",
            },

            "energy_transition": {
                "strategy": "reversing_transition_pivot",
                "original_2020_plan": (
                    "Under Looney: 40% production cut by 2030, massive offshore wind "
                    "and solar buildout. Rebranded 'beyond petroleum' narrative."
                ),
                "current_2024_plan": (
                    "Under Auchincloss: reduced 2030 production cut from 40% to 25%. "
                    "Slashed renewables spending. Sold or paused wind projects. "
                    "Increased oil/gas investment. Activist investor Elliott Management "
                    "took stake in early 2025, pushing for deeper reversal."
                ),
                "elliott_management_involvement": {
                    "stake_pct": "~5% (estimated)",
                    "activist_demands": [
                        "Deeper cuts to renewables spending",
                        "Asset sales (non-core, possibly Castrol)",
                        "Aggressive share buybacks",
                        "Consider strategic alternatives (breakup, US listing)",
                    ],
                    "confidence": "estimated",
                    "source": "Reuters, FT reports Feb 2025",
                },
                "confidence": "confirmed",
                "trading_signal": (
                    "BP is a deep value / activist play. Elliott involvement "
                    "creates catalyst for re-rating. Most leveraged to an oil "
                    "price recovery among majors."
                ),
            },

            "key_assets": [
                {
                    "asset": "Gulf of Mexico deepwater",
                    "type": "upstream",
                    "production_boed": 300_000,
                    "notes": "Still paying Deepwater Horizon costs (~$70B total). Macondo well explosion 2010.",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Azeri-Chirag-Gunashli (Azerbaijan)",
                    "type": "upstream",
                    "interest_pct": 30.37,
                    "production_boed": 400_000,
                    "notes": "BTC pipeline operator. Strategic corridor avoiding Russia.",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Rosneft stake (19.75%)",
                    "type": "equity_investment",
                    "status": "divesting",
                    "notes": (
                        "BP announced exit from 19.75% Rosneft stake after Russia "
                        "invaded Ukraine (Feb 2022). Took $25.5B writedown. Cannot "
                        "sell due to sanctions — stake effectively stranded."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "controversies": [
                {
                    "issue": "Deepwater Horizon (2010)",
                    "description": "11 deaths, largest marine oil spill in history. Total costs ~$70B. Ongoing environmental monitoring.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Rosneft stranded asset",
                    "description": "$25.5B writedown on 19.75% Rosneft stake. Cannot sell due to sanctions.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "CEO scandal",
                    "description": "Bernard Looney resigned Sept 2023 over undisclosed relationships.",
                    "confidence": "confirmed",
                },
            ],

            "lobbying": {
                "annual_spend_usd": 6_100_000,
                "year": 2023,
                "confidence": "estimated",
                "source": "OpenSecrets",
            },

            "pac_contributions": {
                "total_2024_cycle_usd": 800_000,
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "trading_signals": [
                {
                    "signal": "Elliott activist catalyst",
                    "direction": "bullish",
                    "timeframe": "2025",
                    "description": "Activist pressure likely forces asset sales, buybacks, potential strategic review. Classic activist value unlock.",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Deepest discount in sector",
                    "direction": "bullish",
                    "timeframe": "medium_term",
                    "description": "Trades at ~4x EV/EBITDA vs XOM at ~7x. Persistent underperformance creates mean-reversion opportunity.",
                    "confidence": "derived",
                },
            ],
        },

        # ── TOTALENERGIES (TTE) ──────────────────────────────────────
        "TTE": {
            "name": "TotalEnergies SE",
            "ticker": "TTE",
            "exchange": "NYSE / Euronext Paris",
            "sector": "Integrated Oil & Gas",
            "market_cap_usd": 155_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 218_900_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2024_usd": 15_800_000_000,
            "net_income_confidence": "confirmed",
            "employees": 102_000,
            "hq": "Paris (La Defense), France",

            "ceo": {
                "name": "Patrick Pouyanné",
                "title": "Chairman & CEO",
                "appointed": "2014-10-22",
                "total_compensation_2023_eur": 10_000_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "TotalEnergies Annual Report 2023",
                "background": (
                    "Polytechnique and Mines ParisTech graduate. Career Total executive. "
                    "Known for pragmatic dual strategy: invest in both fossil fuels and "
                    "renewables. Maintains Africa/Middle East focus inherited from "
                    "French geopolitical tradition. Controversial for continuing "
                    "operations in Myanmar and Uganda despite human rights concerns."
                ),
                "political_connections": (
                    "Close relationship with French government and Elysee Palace. "
                    "TotalEnergies is seen as instrument of French energy diplomacy "
                    "in Africa and Middle East. Pouyanné personally connected to "
                    "successive French presidents."
                ),
                "confidence": "confirmed",
            },

            "key_assets": [
                {
                    "asset": "Africa upstream portfolio",
                    "type": "upstream",
                    "countries": ["Nigeria", "Angola", "Libya", "Uganda", "Mozambique", "Republic of Congo"],
                    "production_boed": 800_000,
                    "notes": (
                        "Largest Western IOC presence in Africa. EACOP (Uganda-Tanzania) "
                        "pipeline under construction despite massive activist opposition. "
                        "Mozambique LNG (Area 1) paused due to insurgency."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "asset": "Middle East gas (Qatar, UAE, Iraq)",
                    "type": "upstream/LNG",
                    "notes": (
                        "Awarded 9.375% stake in Qatar's North Field South expansion "
                        "(largest LNG project ever). Iraq Basra Gas Company JV. "
                        "Abu Dhabi upstream concessions."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "asset": "Integrated Power (renewables + gas-fired)",
                    "type": "power",
                    "installed_capacity_gw": 22,
                    "notes": "Largest renewables portfolio among European majors. 100GW target by 2030.",
                    "confidence": "confirmed",
                },
            ],

            "energy_transition": {
                "strategy": "dual_strategy_oil_and_renewables",
                "capex_low_carbon_pct": 33,
                "capex_low_carbon_annual_usd": 5_000_000_000,
                "notes": (
                    "Most credible transition among majors: investing $5B/yr in "
                    "low-carbon while maintaining oil/gas production plateau. "
                    "Integrated Power division growing 15%+ annually. But also "
                    "sanctioned new oil projects (Uganda EACOP, Suriname)."
                ),
                "net_zero_target": "Net zero by 2050 (Scope 1+2+3, with carbon sinks)",
                "confidence": "confirmed",
            },

            "controversies": [
                {
                    "issue": "EACOP pipeline (Uganda-Tanzania)",
                    "description": "1,443km heated oil pipeline through sensitive ecosystems. Displacing communities. Major target for climate activists. Multiple European Parliament resolutions condemning project.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Myanmar operations",
                    "description": "Operated Yadana gas field JV with Myanmar military junta. Finally exited 2022 after international pressure. Revenue payments to junta were controversial.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Yemen operations during civil war",
                    "description": "Maintained LNG interests during Yemen civil war.",
                    "confidence": "confirmed",
                },
            ],

            "lobbying": {
                "annual_spend_usd": 4_200_000,
                "year": 2023,
                "jurisdiction": "US (also significant EU lobbying)",
                "confidence": "estimated",
                "source": "OpenSecrets",
            },

            "trading_signals": [
                {
                    "signal": "Best positioned for 'both sides' energy strategy",
                    "direction": "bullish",
                    "timeframe": "medium_term",
                    "description": "Integrated Power division provides growth optionality whether energy transition accelerates or stalls.",
                    "confidence": "derived",
                },
                {
                    "signal": "Africa/Middle East geopolitical premium",
                    "direction": "risk",
                    "timeframe": "ongoing",
                    "description": "Concentrated exposure to politically unstable regions creates tail risk.",
                    "confidence": "inferred",
                },
            ],
        },

        # ── CONOCOPHILLIPS (COP) ──────────────────────────────────────
        "COP": {
            "name": "ConocoPhillips",
            "ticker": "COP",
            "exchange": "NYSE",
            "sector": "Exploration & Production (E&P)",
            "market_cap_usd": 130_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 56_400_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2024_usd": 10_200_000_000,
            "net_income_confidence": "confirmed",
            "employees": 10_400,
            "hq": "Houston, TX",

            "ceo": {
                "name": "Ryan M. Lance",
                "title": "Chairman & CEO",
                "appointed": "2012-05-01",
                "total_compensation_2023_usd": 25_000_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2024",
                "background": (
                    "Career ConocoPhillips executive. Mechanical engineering degree. "
                    "Architect of pure-play E&P strategy post-2012 refining spinoff. "
                    "Known for disciplined capital returns and low-cost supply thesis."
                ),
                "confidence": "confirmed",
            },

            "major_acquisitions": [
                {
                    "target": "Marathon Oil Corporation",
                    "ticker": "MRO",
                    "value_usd": 22_500_000_000,
                    "announced": "2024-05-29",
                    "closed": "2024-11",
                    "type": "all_stock",
                    "strategic_rationale": (
                        "Added Eagle Ford, Bakken, Permian acreage. Continued "
                        "COP's consolidation strategy. Follows $9.5B Concho Resources "
                        "(2021) and $300M Shell Permian acquisition."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "key_assets": [
                {
                    "asset": "Lower 48 (Permian, Eagle Ford, Bakken, SCOOP/STACK)",
                    "type": "upstream",
                    "production_boed": 1_100_000,
                    "notes": "Largest independent E&P in the world by production",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Alaska (Willow project)",
                    "type": "upstream",
                    "production_target_boed": 180_000,
                    "notes": (
                        "Willow approved March 2023 after Biden administration "
                        "controversy. $8B+ investment. First oil expected ~2029. "
                        "Environmental groups strongly opposed."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "asset": "Australia LNG (APLNG, Darwin)",
                    "type": "LNG",
                    "notes": "37.5% APLNG interest. Provides Asian gas exposure.",
                    "confidence": "confirmed",
                },
                {
                    "asset": "Norway / North Sea",
                    "type": "upstream",
                    "production_boed": 100_000,
                    "confidence": "confirmed",
                },
            ],

            "energy_transition": {
                "strategy": "pure_play_fossil_with_efficiency",
                "capex_low_carbon_pct": 1,
                "notes": "No meaningful renewables investment. Focus on emissions intensity reduction from existing operations.",
                "net_zero_target": "Net zero Scope 1+2 by 2050. Zero Scope 3 commitment.",
                "confidence": "confirmed",
            },

            "lobbying": {
                "annual_spend_usd": 5_600_000,
                "year": 2023,
                "confidence": "confirmed",
                "source": "OpenSecrets",
            },

            "pac_contributions": {
                "total_2024_cycle_usd": 1_100_000,
                "split_pct": {"republican": 72, "democrat": 28},
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "trading_signals": [
                {
                    "signal": "Consolidation king premium",
                    "direction": "bullish",
                    "timeframe": "medium_term",
                    "description": "COP's serial acquisition strategy (Concho, Shell Permian, Marathon Oil) creates operational leverage and cost synergies.",
                    "confidence": "derived",
                },
                {
                    "signal": "Capital returns discipline",
                    "direction": "bullish",
                    "timeframe": "ongoing",
                    "description": "Targets returning 30%+ of CFO to shareholders. One of highest payout rates in sector.",
                    "confidence": "confirmed",
                },
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════
    # OPEC+ POWER PLAYERS
    # ══════════════════════════════════════════════════════════════════

    "opec_plus": {

        # ── SAUDI ARAMCO ─────────────────────────────────────────────
        "SAUDI_ARAMCO": {
            "name": "Saudi Arabian Oil Company (Saudi Aramco)",
            "ticker": "2222.SR",
            "exchange": "Tadawul (Saudi Exchange)",
            "sector": "National Oil Company (NOC)",
            "market_cap_usd": 1_800_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 404_000_000_000,
            "revenue_confidence": "estimated",
            "net_income_2024_usd": 106_000_000_000,
            "net_income_confidence": "estimated",
            "employees": 73_000,
            "hq": "Dhahran, Saudi Arabia",
            "production_boed": 9_000_000,
            "spare_capacity_boed": 3_000_000,
            "spare_capacity_confidence": "estimated",
            "max_sustained_capacity_boed": 12_000_000,

            "ceo": {
                "name": "Amin H. Nasser",
                "title": "President & CEO",
                "appointed": "2015-09-01",
                "background": (
                    "Career Aramco executive since 1982. Petroleum engineering. "
                    "Managed company through 2019 IPO, Abqaiq drone attacks, "
                    "COVID crash, and OPEC+ negotiations. Reports directly to "
                    "Crown Prince MBS."
                ),
                "confidence": "confirmed",
            },

            "controlling_authority": {
                "name": "Mohammed bin Salman (MBS)",
                "title": "Crown Prince & Prime Minister of Saudi Arabia",
                "control_mechanism": (
                    "Saudi government owns 98.2% of Aramco. MBS personally controls "
                    "all energy policy. Aramco's Board Chair is PIF governor Yasir "
                    "Al-Rumayyan (also on PIF board). Oil revenue funds Vision 2030 "
                    "and MBS's mega-projects (NEOM, The Line, etc.)."
                ),
                "mbs_priorities": [
                    "Fund Vision 2030 diversification ($1T+ commitment)",
                    "Maintain geopolitical leverage via spare capacity",
                    "Control OPEC+ to manage oil prices in $80-100 range",
                    "Use Aramco dividends to fund PIF sovereign wealth investments",
                ],
                "confidence": "confirmed",
            },

            "ipo_history": {
                "ipo_date": "2019-12-11",
                "ipo_valuation_usd": 1_700_000_000_000,
                "shares_floated_pct": 1.73,
                "secondary_offering_2024": {
                    "date": "2024-06",
                    "shares_sold_pct": 0.64,
                    "raised_usd": 11_200_000_000,
                    "purpose": "Fund PIF / Vision 2030 projects",
                },
                "mbs_original_target_valuation": 2_000_000_000_000,
                "confidence": "confirmed",
            },

            "opec_role": {
                "role": "De facto OPEC leader and swing producer",
                "strategy": (
                    "MBS uses spare capacity as geopolitical weapon and market "
                    "management tool. Willing to cut production to support prices "
                    "(2023-2024 voluntary cuts of 1M bpd). But also willing to "
                    "flood market to punish cheaters (2020 price war vs Russia, "
                    "2014-2016 vs US shale)."
                ),
                "current_voluntary_cuts_boed": 1_000_000,
                "confidence": "confirmed",
            },

            "geopolitical_risks": [
                {
                    "risk": "Houthi/Iran proxy attacks on infrastructure",
                    "severity": "high",
                    "description": "Abqaiq/Khurais drone attack (2019) temporarily halved Saudi output. Yemen Houthis continue attacks on Saudi infrastructure and Red Sea shipping.",
                    "confidence": "confirmed",
                },
                {
                    "risk": "US-Saudi relationship volatility",
                    "severity": "medium",
                    "description": "Relationship strained under Biden (Khashoggi, OPEC+ cuts opposing US wishes). Could improve or deteriorate under future administrations.",
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "OPEC+ production decisions",
                    "direction": "macro_driver",
                    "timeframe": "ongoing",
                    "description": "Every OPEC+ meeting (monthly JMMC, quarterly ministerial) is a vol event for all energy names.",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Saudi fiscal breakeven oil price",
                    "direction": "floor_signal",
                    "timeframe": "ongoing",
                    "description": "Saudi needs ~$85-90/bbl Brent to balance budget (incl. Vision 2030). Below this, MBS pressured to cut production further.",
                    "confidence": "estimated",
                },
            ],
        },

        # ── UAE ADNOC ────────────────────────────────────────────────
        "ADNOC": {
            "name": "Abu Dhabi National Oil Company (ADNOC)",
            "sector": "National Oil Company (NOC)",
            "ownership": "100% Abu Dhabi Government",
            "revenue_2023_usd": 100_000_000_000,
            "revenue_confidence": "estimated",
            "employees": 60_000,
            "hq": "Abu Dhabi, UAE",
            "production_boed": 4_000_000,
            "target_capacity_2027_boed": 5_000_000,

            "ceo": {
                "name": "Sultan Ahmed Al Jaber",
                "title": "Group CEO of ADNOC; UAE Minister of Industry; COP28 President",
                "appointed_adnoc": "2016-02-07",
                "background": (
                    "Simultaneously runs ADNOC ($150B expansion program), served as "
                    "COP28 UN climate conference president (Dec 2023), chairs Masdar "
                    "(UAE clean energy company), and is UAE Industry Minister. His "
                    "appointment as COP28 president was deeply controversial — "
                    "critics said it was a conflict of interest to have an oil "
                    "executive lead climate negotiations."
                ),
                "cop28_controversy": (
                    "Leaked documents showed ADNOC planned to use COP28 meetings "
                    "to discuss oil deals with foreign governments. Al Jaber denied "
                    "claims but BBC/Centre for Climate Reporting published evidence. "
                    "COP28 ultimately agreed on 'transitioning away from fossil fuels' "
                    "— first time fossil fuels mentioned in COP text."
                ),
                "confidence": "confirmed",
            },

            "expansion_program": {
                "total_investment_usd": 150_000_000_000,
                "period": "2024-2030",
                "key_projects": [
                    "Capacity expansion to 5M bpd by 2027",
                    "Ruwais refinery downstream expansion",
                    "Hail & Ghasha sour gas mega-project",
                    "International upstream acquisitions (acquired Wintershall Dea assets)",
                    "ADNOC Drilling, ADNOC Gas, Borouge IPOs (capital recycling)",
                ],
                "confidence": "confirmed",
            },

            "international_acquisitions": [
                {
                    "target": "Wintershall Dea upstream assets",
                    "value_usd": 11_700_000_000,
                    "year": 2024,
                    "notes": "From BASF. European and North African upstream. ADNOC going global.",
                    "confidence": "confirmed",
                },
                {
                    "target": "Covestro AG (chemicals)",
                    "value_eur": 11_700_000_000,
                    "year": 2024,
                    "notes": "Bid for German specialty chemicals company. Downstream integration.",
                    "confidence": "confirmed",
                },
            ],

            "geopolitical_risks": [
                {
                    "risk": "Iran/Houthi Red Sea disruption",
                    "severity": "medium",
                    "description": "UAE ports and shipping lanes exposed to Houthi attacks and Iran tensions. Fujairah oil terminal outside Strait of Hormuz provides some hedge.",
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "ADNOC IPO wave",
                    "direction": "watch",
                    "timeframe": "2024-2026",
                    "description": "Series of subsidiary IPOs (Drilling, Gas, Logistics) creating investable vehicles and capital recycling. Watch for ADNOC upstream IPO.",
                    "confidence": "derived",
                },
                {
                    "signal": "UAE breaking OPEC+ quotas",
                    "direction": "bearish_oil",
                    "timeframe": "2025+",
                    "description": "UAE successfully lobbied for higher OPEC+ baseline. May push for more, causing intra-OPEC tension with Saudi Arabia.",
                    "confidence": "inferred",
                },
            ],
        },

        # ── RUSSIA (ROSNEFT / GAZPROM) ───────────────────────────────
        "ROSNEFT": {
            "name": "Rosneft Oil Company",
            "ticker": "ROSN.MM (Moscow Exchange, restricted)",
            "sector": "National Oil Company (NOC)",
            "ownership": "Rosneftegaz (state holding) 40.4%, BP 19.75% (stranded), QIA 18.93%",
            "revenue_2023_usd": 108_000_000_000,
            "revenue_confidence": "estimated",
            "production_boed": 4_600_000,
            "employees": 340_000,
            "hq": "Moscow, Russia",

            "ceo": {
                "name": "Igor Ivanovich Sechin",
                "title": "CEO & Chairman of the Management Board",
                "appointed": "2012-05-23",
                "background": (
                    "Former Deputy Prime Minister under Putin. KGB/FSB-linked (military "
                    "intelligence background). Considered Putin's most powerful economic "
                    "ally. Personally sanctioned by US, EU, UK, Canada, Australia. "
                    "Known as 'Darth Vader' of Russian energy. Architect of Rosneft's "
                    "aggressive expansion including seizure of Yukos assets."
                ),
                "sanctions_status": "Personally sanctioned (US EO 13661, EU, UK)",
                "putin_relationship": "Inner circle. One of Putin's closest confidants since 1990s St. Petersburg city government.",
                "confidence": "confirmed",
            },

            "sanctions_impact": {
                "western_sanctions": [
                    "Price cap on Russian oil ($60/bbl, Dec 2022)",
                    "EU embargo on Russian crude (Dec 2022) and refined products (Feb 2023)",
                    "US/EU sanctions on Rosneft entities and executives",
                    "Technology export controls limiting oilfield service access",
                ],
                "sanctions_evasion": {
                    "shadow_fleet": (
                        "Russia assembled 600+ 'shadow fleet' tankers (old, poorly "
                        "insured) to ship oil above price cap. Ships often disable "
                        "AIS transponders. Environmental risk from aging vessels."
                    ),
                    "india_china_rerouting": (
                        "Russian crude redirected to India and China at discounted "
                        "prices ($10-20/bbl below Brent). India refiners (Reliance, "
                        "Nayara) process and re-export as products to Europe."
                    ),
                    "confidence": "confirmed",
                    "source": "CREA, Kpler, Reuters tracking",
                },
                "revenue_impact": "Reduced but not eliminated. Russia earning ~$15B/month from oil despite sanctions (vs ~$20B pre-invasion).",
                "confidence": "estimated",
            },

            "key_assets": [
                {
                    "asset": "Vostok Oil mega-project (Arctic)",
                    "type": "upstream",
                    "estimated_reserves_boe": 6_000_000_000,
                    "target_production_boed": 2_000_000,
                    "status": "under_development",
                    "notes": (
                        "Sechin's flagship project. $170B estimated cost. Arctic "
                        "conditions. Western oilfield service companies withdrew. "
                        "Relying on Chinese and domestic technology. Trafigura "
                        "had pre-paid for off-take (controversial)."
                    ),
                    "confidence": "estimated",
                },
            ],

            "geopolitical_risks": [
                {
                    "risk": "Escalating sanctions",
                    "severity": "high",
                    "description": "Further sanctions tightening (shadow fleet crackdown, secondary sanctions on India/China refiners) could reduce Russian revenues.",
                    "confidence": "confirmed",
                },
            ],

            "trading_signals": [
                {
                    "signal": "Russian supply disruption",
                    "direction": "bullish_oil",
                    "timeframe": "event_driven",
                    "description": "Any effective sanctions enforcement or actual supply disruption is bullish for global oil prices.",
                    "confidence": "derived",
                },
                {
                    "signal": "India/China discount arbitrage",
                    "direction": "watch",
                    "timeframe": "ongoing",
                    "description": "Urals-Brent spread is a proxy for sanctions effectiveness. Narrowing spread = weaker sanctions.",
                    "confidence": "derived",
                },
            ],
        },

        "GAZPROM": {
            "name": "Gazprom PJSC",
            "ticker": "GAZP.MM (Moscow Exchange, restricted)",
            "sector": "National Gas Company",
            "ownership": "Russian Federation 50.23%",
            "revenue_2023_usd": 79_000_000_000,
            "revenue_confidence": "estimated",
            "employees": 497_000,
            "hq": "St. Petersburg, Russia",

            "ceo": {
                "name": "Alexey Miller",
                "title": "Chairman of the Management Committee",
                "appointed": "2001-05-30",
                "background": "Putin loyalist from St. Petersburg. Runs Gazprom as instrument of Russian foreign policy.",
                "sanctions_status": "Personally sanctioned (EU, UK, Australia)",
                "confidence": "confirmed",
            },

            "post_invasion_impact": {
                "european_gas_loss": (
                    "Gazprom lost ~80% of European gas market share after Russia "
                    "weaponized gas supplies in 2022. Nord Stream pipelines sabotaged "
                    "(Sept 2022). European gas imports from Russia fell from ~40% to ~8% "
                    "of supply. Gazprom reported its first annual loss in 25 years in 2023."
                ),
                "nord_stream_sabotage": "Both Nord Stream 1 and 2 damaged by explosions Sept 2022. German investigation ongoing. Multiple theories (Ukraine, Russia, US).",
                "china_pivot": (
                    "Power of Siberia 1 pipeline to China: 38 bcm/yr capacity. "
                    "Power of Siberia 2 (via Mongolia): 50 bcm/yr proposed but "
                    "negotiations stalled. China extracting low prices."
                ),
                "confidence": "confirmed",
            },

            "trading_signals": [
                {
                    "signal": "European gas price proxy",
                    "direction": "macro",
                    "timeframe": "ongoing",
                    "description": "TTF gas prices reflect Gazprom supply loss. Any resumption of Russian gas flows (post-conflict scenario) would crash European gas prices.",
                    "confidence": "derived",
                },
            ],
        },

        # ── IRAQ, KUWAIT, NIGERIA ────────────────────────────────────
        "IRAQ": {
            "name": "Republic of Iraq — Oil Sector",
            "sector": "National Oil Production",
            "production_boed": 4_400_000,
            "opec_quota_boed": 4_000_000,
            "compliance_issue": "Chronic over-producer. Consistently exceeds OPEC quota by 200-400K bpd.",
            "key_fields": ["Rumaila (BP operated)", "West Qurna 2 (Lukoil)", "Majnoon", "Zubair (ENI)"],
            "revenue_dependency": "95%+ of government revenue from oil",
            "geopolitical_risks": [
                {
                    "risk": "Kurdistan Regional Government (KRG) pipeline dispute",
                    "severity": "medium",
                    "description": "Iraq-Turkey pipeline from KRG shut since March 2023 due to ICC arbitration ruling. ~450K bpd offline.",
                    "confidence": "confirmed",
                },
                {
                    "risk": "Iran influence on Iraqi politics",
                    "severity": "medium",
                    "description": "Iran-backed militias influence oil ministry and southern operations.",
                    "confidence": "estimated",
                },
            ],
            "confidence": "confirmed",
        },

        "KUWAIT": {
            "name": "Kuwait — Kuwait Petroleum Corporation (KPC)",
            "sector": "National Oil Company",
            "production_boed": 2_500_000,
            "opec_quota_boed": 2_400_000,
            "key_entity": "Kuwait Petroleum Corporation (KPC)",
            "notes": (
                "Conservative OPEC member. Generally compliant with quotas. "
                "Neutral Zone (shared with Saudi) production resumed 2020 after "
                "5-year shutdown. KPC downstream expansion (Al-Zour refinery, "
                "650K bpd, largest in Middle East)."
            ),
            "confidence": "confirmed",
        },

        "NIGERIA": {
            "name": "Nigeria — NNPC Ltd",
            "sector": "National Oil Company",
            "production_boed": 1_500_000,
            "opec_quota_boed": 1_780_000,
            "compliance_issue": "Under-produces vs quota due to theft, sabotage, and underinvestment.",
            "key_entity": "Nigerian National Petroleum Company Limited (NNPC Ltd)",
            "ceo": {"name": "Mele Kyari", "title": "Group CEO, NNPC Ltd", "confidence": "confirmed"},
            "key_issues": [
                {
                    "issue": "Crude oil theft",
                    "description": "100-400K bpd estimated stolen from pipelines. Organized crime networks, political protection.",
                    "confidence": "estimated",
                },
                {
                    "issue": "IOC divestment",
                    "description": "Shell, ExxonMobil, others divesting onshore/shallow water assets to Nigerian companies. Environmental liabilities being transferred.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Dangote Refinery",
                    "description": "650K bpd refinery (Aliko Dangote). Largest single-train refinery in world. Aims to end Nigeria's fuel imports paradox. Ramp-up ongoing.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Petroleum Industry Act (PIA) 2021",
                    "description": "Restructured NNPC, created host community fund. Mixed reviews on attracting new investment.",
                    "confidence": "confirmed",
                },
            ],
            "confidence": "confirmed",
        },
    },

    # ══════════════════════════════════════════════════════════════════
    # OPEC+ DYNAMICS (CROSS-CUTTING)
    # ══════════════════════════════════════════════════════════════════

    "opec_plus_dynamics": {
        "members_opec": 12,
        "members_plus": 10,
        "total_production_boed": 42_000_000,
        "global_share_pct": 40,
        "key_tensions": [
            {
                "tension": "Saudi-UAE baseline dispute",
                "description": "UAE wants higher baseline (earned by capacity expansion). Saudi wants compliance. Ongoing friction since 2021.",
                "confidence": "confirmed",
            },
            {
                "tension": "Russia compliance accounting",
                "description": "Russia self-reports production data. Actual compliance with cuts is disputed. Sanctions make verification harder.",
                "confidence": "estimated",
            },
            {
                "tension": "Iraq chronic overproduction",
                "description": "Iraq consistently exceeds quota. Pledges 'compensatory cuts' but rarely delivers.",
                "confidence": "confirmed",
            },
            {
                "tension": "US shale response function",
                "description": "OPEC+ cuts lose effectiveness when US shale rapidly fills supply gaps. Limits OPEC pricing power.",
                "confidence": "confirmed",
            },
        ],
        "meeting_schedule": "JMMC monthly, full ministerial quarterly",
        "key_dates_2025": [
            "OPEC+ JMMC meets first week of each month",
            "Full ministerial: March, June, September, December (approx.)",
        ],
        "confidence": "confirmed",
    },

    # ══════════════════════════════════════════════════════════════════
    # ENERGY TRADERS
    # ══════════════════════════════════════════════════════════════════

    "energy_traders": {

        # ── VITOL ────────────────────────────────────────────────────
        "VITOL": {
            "name": "Vitol Group",
            "type": "private",
            "sector": "Commodity Trading",
            "hq": "Geneva, Switzerland (also Rotterdam, Singapore, Houston)",
            "revenue_2023_usd": 270_000_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "Vitol annual report",
            "net_income_2023_usd": 13_000_000_000,
            "net_income_confidence": "estimated",
            "employees": 6_500,
            "trading_volumes": "8M bpd crude and products (largest independent oil trader in the world)",
            "founded": 1966,

            "ceo": {
                "name": "Russell Hardy",
                "title": "CEO",
                "appointed": "2018",
                "background": "Joined Vitol 1993. Previously headed European oil trading.",
                "confidence": "confirmed",
            },

            "legacy_leadership": {
                "name": "Ian Taylor",
                "role": "Former CEO (1995-2018), then Chairman until death 2020",
                "notes": (
                    "Taylor built Vitol into the world's largest independent trader. "
                    "Known for aggressive deal-making, including controversial trades "
                    "with Libya during civil war and Iraq under sanctions. Donated "
                    "heavily to UK Conservative Party. Died of cancer 2020."
                ),
                "confidence": "confirmed",
            },

            "controversies": [
                {
                    "issue": "Libyan oil trading during civil war",
                    "description": "Traded oil with Libyan rebels during 2011 civil war. Provided fuel that helped overthrow Gaddafi. Legal gray area.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Brazil Petrobras bribery (Operation Car Wash)",
                    "description": "Vitol paid $164M to US DOJ and CFTC in 2020 to settle charges of bribing Petrobras officials in Brazil, Ecuador, and Mexico.",
                    "settlement_usd": 164_000_000,
                    "confidence": "confirmed",
                },
                {
                    "issue": "Iraqi oil-for-food programme",
                    "description": "Implicated in Volcker Report on UN Oil-for-Food Programme irregularities.",
                    "confidence": "confirmed",
                },
            ],

            "offshore_structures": {
                "jurisdictions": ["Switzerland", "Netherlands", "Singapore", "Bermuda", "Bahamas"],
                "ownership_structure": "Employee-owned partnership. ~450 shareholders (current and former employees).",
                "transparency": "Limited. No public equity. Annual report published but not audited to public company standards.",
                "confidence": "estimated",
            },

            "trading_signals": [
                {
                    "signal": "Vitol storage/freight activity",
                    "direction": "indicator",
                    "description": "Vitol's contango/backwardation positioning (floating storage bookings) is a leading indicator of oil market structure.",
                    "confidence": "inferred",
                },
            ],
        },

        # ── GLENCORE ─────────────────────────────────────────────────
        "GLEN": {
            "name": "Glencore plc",
            "ticker": "GLEN.L",
            "exchange": "LSE / JSE",
            "sector": "Commodity Trading & Mining",
            "market_cap_usd": 58_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2023_usd": 217_800_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2023_usd": 4_300_000_000,
            "employees": 152_000,
            "hq": "Baar, Switzerland",

            "ceo": {
                "name": "Gary Nagle",
                "title": "CEO",
                "appointed": "2021-07-01",
                "nationality": "South African",
                "background": (
                    "Coal division head before becoming CEO. Succeeded Ivan Glasenberg "
                    "(who built modern Glencore). Oversaw guilty plea to bribery charges "
                    "and $1.1B settlement."
                ),
                "confidence": "confirmed",
            },

            "legacy_leadership": {
                "name": "Ivan Glasenberg",
                "role": "Former CEO (2002-2021)",
                "notes": (
                    "Israeli-South African. Architect of Glencore IPO (2011) and "
                    "Xstrata merger. One of world's richest men. Retired 2021. "
                    "Previously: Marc Rich + Co (Glencore's predecessor, founded "
                    "by Marc Rich — pardoned by Bill Clinton in controversial "
                    "last-day pardon)."
                ),
                "marc_rich_history": (
                    "Glencore was originally Marc Rich + Co AG. Rich was indicted "
                    "for tax evasion and illegal oil trading with Iran during hostage "
                    "crisis. Fled to Switzerland. Controversially pardoned by Clinton "
                    "on last day in office (Jan 2001). Rich's ex-wife Denise donated "
                    "heavily to Democrats and Clinton library."
                ),
                "confidence": "confirmed",
            },

            "controversies": [
                {
                    "issue": "DOJ/CFTC bribery guilty plea ($1.1B)",
                    "description": (
                        "Pled guilty in 2022 to bribery and market manipulation charges "
                        "across Nigeria, Cameroon, Ivory Coast, Equatorial Guinea, Brazil, "
                        "Venezuela, and DRC. Paid $1.1B in fines (US DOJ, CFTC, UK SFO). "
                        "Independent compliance monitor imposed."
                    ),
                    "settlement_total_usd": 1_100_000_000,
                    "confidence": "confirmed",
                },
                {
                    "issue": "DRC cobalt/copper operations",
                    "description": "Mutanda and Katanga Mining operations linked to child labor concerns, pollution, community displacement.",
                    "confidence": "estimated",
                },
                {
                    "issue": "Coal retention controversy",
                    "description": "Despite climate pledges, Glencore retained massive coal portfolio (world's largest thermal coal exporter). Framed as 'responsible wind-down' but coal profits fund dividends.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Sanctions exposure via Russian aluminum",
                    "description": "Previously held Rusal stake (divested). Ongoing trading relationships with Russian entities under scrutiny.",
                    "confidence": "estimated",
                },
            ],

            "offshore_structures": {
                "jurisdictions": ["Switzerland", "Jersey", "BVI", "Bermuda", "Luxembourg"],
                "tax_structure": "Complex multi-jurisdictional structure. Swiss trading arm, Jersey holding entities. Effective tax rate historically low.",
                "paradise_papers_mentions": "Multiple Glencore entities appeared in Paradise Papers (2017).",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {
                    "signal": "Coal price leverage",
                    "direction": "indicator",
                    "description": "GLEN is world's largest thermal coal exporter. Coal price movements disproportionately impact stock.",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Copper/cobalt EV exposure",
                    "direction": "bullish_secular",
                    "description": "Mining arm has massive copper and cobalt resources critical for energy transition (batteries, EVs, grid).",
                    "confidence": "derived",
                },
                {
                    "signal": "Compliance monitor overhang",
                    "direction": "risk",
                    "timeframe": "through 2025",
                    "description": "DOJ-imposed compliance monitor restricts aggressive trading strategies.",
                    "confidence": "confirmed",
                },
            ],
        },

        # ── TRAFIGURA ────────────────────────────────────────────────
        "TRAFIGURA": {
            "name": "Trafigura Group Pte. Ltd.",
            "type": "private",
            "sector": "Commodity Trading",
            "hq": "Singapore (also Geneva)",
            "revenue_2023_usd": 244_000_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "Trafigura annual report",
            "net_income_2023_usd": 7_000_000_000,
            "net_income_confidence": "estimated",
            "employees": 13_700,
            "trading_volumes": "7.6M bpd oil and products",
            "founded": 1993,

            "ceo": {
                "name": "Jeremy Weir",
                "title": "Executive Chairman & CEO",
                "appointed": "2014",
                "nationality": "British",
                "background": (
                    "Former Marc Rich trader (like Glencore founders). Built "
                    "Trafigura into world's second-largest independent oil trader."
                ),
                "confidence": "confirmed",
            },

            "controversies": [
                {
                    "issue": "Nickel warehouse manipulation allegations",
                    "description": (
                        "Alleged manipulation of LME nickel warehouse stocks. Trafigura "
                        "accumulated large physical nickel positions through its Impala "
                        "Terminals subsidiary. Part of broader scrutiny of commodity "
                        "warehouse practices."
                    ),
                    "confidence": "rumored",
                },
                {
                    "issue": "Ivory Coast toxic waste (Probo Koala, 2006)",
                    "description": (
                        "Trafigura chartered ship Probo Koala that dumped toxic waste "
                        "in Abidjan, Ivory Coast. 17 deaths, 100K+ sought medical "
                        "treatment. Trafigura settled for $198M without admitting "
                        "liability. Largest toxic waste scandal in modern history."
                    ),
                    "settlement_usd": 198_000_000,
                    "confidence": "confirmed",
                },
                {
                    "issue": "Russian oil trading post-invasion",
                    "description": "Traded significant volumes of Russian oil in 2022 before publicly stepping back. Extent of ongoing indirect exposure disputed.",
                    "confidence": "estimated",
                },
                {
                    "issue": "Mongstad fraud (2024)",
                    "description": "Former Trafigura oil trader accused of fraud related to Mongstad refinery transactions. Internal investigation.",
                    "confidence": "confirmed",
                },
            ],

            "offshore_structures": {
                "jurisdictions": ["Singapore", "Switzerland", "Malta", "Bermuda", "Netherlands"],
                "ownership": "Employee-owned. ~600 shareholders.",
                "transparency": "Limited. Private company. Annual report published.",
                "confidence": "estimated",
            },

            "trading_signals": [
                {
                    "signal": "Metals market positioning",
                    "direction": "indicator",
                    "description": "Trafigura's metals trading (especially zinc, copper, nickel) positions can move physical premiums.",
                    "confidence": "inferred",
                },
            ],
        },

        # ── GUNVOR ───────────────────────────────────────────────────
        "GUNVOR": {
            "name": "Gunvor Group Ltd",
            "type": "private",
            "sector": "Commodity Trading",
            "hq": "Geneva, Switzerland",
            "revenue_2023_usd": 110_000_000_000,
            "revenue_confidence": "estimated",
            "employees": 1_800,
            "trading_volumes": "~3M bpd",
            "founded": 2000,

            "ceo": {
                "name": "Torbjörn Törnqvist",
                "title": "CEO & Co-Founder",
                "nationality": "Swedish",
                "background": (
                    "Former oil trader at Scandinavian trading houses. Co-founded "
                    "Gunvor in 2000 with Gennady Timchenko (Russian oligarch, "
                    "Putin associate). Törnqvist bought out Timchenko's stake in "
                    "2014, days before US sanctions hit Timchenko."
                ),
                "confidence": "confirmed",
            },

            "russian_origins": {
                "co_founder": "Gennady Timchenko",
                "timchenko_status": (
                    "Russian-Finnish billionaire. Close Putin associate. Sanctioned "
                    "by US (March 2014), EU, UK. Sold his 44% Gunvor stake to "
                    "Törnqvist just days before sanctions hit (March 20, 2014). "
                    "Timing raised questions about prior knowledge of sanctions."
                ),
                "residual_russian_exposure": (
                    "Gunvor claims no Russian crude trading since 2022. However, "
                    "historical ties and pipeline relationships create reputational "
                    "risk. Some counterparties remain cautious."
                ),
                "confidence": "confirmed",
            },

            "controversies": [
                {
                    "issue": "Timchenko stake sale timing",
                    "description": "Timchenko sold 44% stake days before US sanctions. Investigated but no charges against Gunvor.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Congo-Brazzaville bribery",
                    "description": "Former Gunvor trader convicted in Swiss court of bribing Republic of Congo officials for oil deals. Gunvor paid SFr94M in Swiss penalties.",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Ecuador bribery",
                    "description": "Linked to payments to PetroEcuador officials. Part of broader commodity trading bribery investigations.",
                    "confidence": "estimated",
                },
            ],

            "offshore_structures": {
                "jurisdictions": ["Switzerland", "Cyprus", "BVI", "Singapore"],
                "transparency": "Private. Limited disclosure.",
                "confidence": "estimated",
            },

            "trading_signals": [
                {
                    "signal": "Russian crude flow indicator",
                    "direction": "watch",
                    "description": "Gunvor's historical Russian ties make it a bellwether for Russian crude market access changes.",
                    "confidence": "inferred",
                },
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════
    # RENEWABLES
    # ══════════════════════════════════════════════════════════════════

    "renewables": {

        # ── NEXTERA ENERGY (NEE) ─────────────────────────────────────
        "NEE": {
            "name": "NextEra Energy, Inc.",
            "ticker": "NEE",
            "exchange": "NYSE",
            "sector": "Utilities — Renewable Energy",
            "market_cap_usd": 165_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 24_700_000_000,
            "revenue_confidence": "confirmed",
            "net_income_2024_usd": 7_300_000_000,
            "net_income_confidence": "confirmed",
            "employees": 16_800,
            "hq": "Juno Beach, FL",

            "ceo": {
                "name": "John Ketchum",
                "title": "Chairman, President & CEO",
                "appointed": "2022-03-01",
                "total_compensation_2023_usd": 24_600_000,
                "compensation_confidence": "confirmed",
                "background": "Former CFO. Civil engineering + MBA. Oversaw massive wind/solar buildout.",
                "confidence": "confirmed",
            },

            "key_assets": [
                {
                    "asset": "NextEra Energy Resources (NEER)",
                    "type": "wind_solar_storage",
                    "installed_capacity_gw": 34,
                    "notes": (
                        "Largest generator of wind and solar energy in the world. "
                        "~21 GW wind, ~7 GW solar, ~6 GW storage/other. Massive "
                        "pipeline of projects. Benefits from IRA tax credits (PTC/ITC)."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "asset": "Florida Power & Light (FPL)",
                    "type": "regulated_utility",
                    "customers": 12_000_000,
                    "notes": "Largest electric utility in Florida. Regulated rate base provides stable earnings.",
                    "confidence": "confirmed",
                },
            ],

            "political_connections": {
                "ira_beneficiary": (
                    "Massive beneficiary of Inflation Reduction Act (IRA) tax credits. "
                    "PTC, ITC, and tech-neutral credits worth billions over next decade. "
                    "Any IRA repeal/modification is existential risk."
                ),
                "florida_politics": (
                    "FPL is politically influential in Florida. Investigated by "
                    "Orlando Sentinel for sponsoring 'ghost candidates' in state "
                    "senate races to siphon votes from opponents of rate hikes."
                ),
                "confidence": "estimated",
            },

            "lobbying": {
                "annual_spend_usd": 5_400_000,
                "year": 2023,
                "confidence": "confirmed",
                "source": "OpenSecrets",
            },

            "pac_contributions": {
                "total_2024_cycle_usd": 3_200_000,
                "split_pct": {"republican": 62, "democrat": 38},
                "confidence": "estimated",
                "source": "OpenSecrets.org",
            },

            "trading_signals": [
                {
                    "signal": "IRA policy risk",
                    "direction": "risk",
                    "timeframe": "political_cycle",
                    "description": "NEE stock is a proxy for IRA survival. Any legislative threat to clean energy tax credits hits NEE disproportionately.",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Interest rate sensitivity",
                    "direction": "bearish_in_rising_rates",
                    "timeframe": "ongoing",
                    "description": "As capital-intensive utility/renewables, NEE is highly sensitive to interest rates. Fed rate cuts are bullish.",
                    "confidence": "confirmed",
                },
                {
                    "signal": "Data center power demand",
                    "direction": "bullish",
                    "timeframe": "2024-2030",
                    "description": "AI data center buildout driving unprecedented power demand growth. NEE positioned to supply clean energy PPAs to hyperscalers.",
                    "confidence": "derived",
                },
            ],
        },

        # ── FIRST SOLAR (FSLR) ───────────────────────────────────────
        "FSLR": {
            "name": "First Solar, Inc.",
            "ticker": "FSLR",
            "exchange": "NASDAQ",
            "sector": "Solar Manufacturing",
            "market_cap_usd": 22_000_000_000,
            "market_cap_confidence": "confirmed",
            "revenue_2024_usd": 4_200_000_000,
            "revenue_confidence": "estimated",
            "net_income_2024_usd": 1_100_000_000,
            "net_income_confidence": "estimated",
            "employees": 7_600,
            "hq": "Tempe, AZ",

            "ceo": {
                "name": "Mark Widmar",
                "title": "CEO",
                "appointed": "2016-07-01",
                "background": "Former CFO. CPA background. Oversaw shift to Series 6/7 CdTe thin-film modules and US manufacturing expansion.",
                "confidence": "confirmed",
            },

            "key_assets": [
                {
                    "asset": "US manufacturing capacity",
                    "type": "solar_manufacturing",
                    "capacity_gw": 14,
                    "notes": (
                        "Largest US solar manufacturer. CdTe thin-film technology "
                        "(different from Chinese crystalline silicon). Factories in "
                        "Ohio, Alabama, Louisiana. New India factory. Benefits from "
                        "IRA Section 45X manufacturing credits (~$10/panel)."
                    ),
                    "confidence": "confirmed",
                },
            ],

            "competitive_advantages": [
                "Only vertically integrated US solar manufacturer at scale",
                "CdTe technology avoids polysilicon supply chain (no Xinjiang exposure)",
                "IRA 45X manufacturing credits create $0.17/watt+ advantage",
                "Fully booked through 2026+ with contracted backlog",
            ],

            "political_connections": {
                "ira_beneficiary": "Massive beneficiary of IRA 45X advanced manufacturing credits. Worth ~$700M/year in credits.",
                "tariff_beneficiary": "US anti-dumping/countervailing duties on Chinese solar imports benefit FSLR as domestic manufacturer.",
                "confidence": "confirmed",
            },

            "lobbying": {
                "annual_spend_usd": 1_800_000,
                "year": 2023,
                "confidence": "confirmed",
                "source": "OpenSecrets",
            },

            "trading_signals": [
                {
                    "signal": "IRA manufacturing credits",
                    "direction": "bullish_if_ira_survives",
                    "timeframe": "2024-2032",
                    "description": "FSLR is THE IRA manufacturing play. 45X credits worth $700M+/yr. Any IRA modification is binary risk.",
                    "confidence": "confirmed",
                },
                {
                    "signal": "China tariff escalation",
                    "direction": "bullish",
                    "timeframe": "event_driven",
                    "description": "Additional tariffs on Chinese solar (including Southeast Asian transshipment) directly benefits FSLR.",
                    "confidence": "derived",
                },
                {
                    "signal": "Technology risk",
                    "direction": "risk",
                    "timeframe": "long_term",
                    "description": "CdTe thin-film is less efficient than crystalline silicon. If perovskite or next-gen technologies mature, FSLR's technology moat could erode.",
                    "confidence": "inferred",
                },
            ],
        },

        # ── TESLA ENERGY ─────────────────────────────────────────────
        "TSLA_ENERGY": {
            "name": "Tesla Energy (division of Tesla, Inc.)",
            "parent_ticker": "TSLA",
            "exchange": "NASDAQ",
            "sector": "Grid Storage / Energy",
            "division_revenue_2024_usd": 10_400_000_000,
            "division_revenue_confidence": "confirmed",
            "division_gross_margin_pct": 24.6,
            "parent_market_cap_usd": 790_000_000_000,

            "key_person": {
                "name": "Elon Musk",
                "title": "CEO of Tesla, Inc.",
                "relevance_to_energy": (
                    "Tesla Energy is growing faster than autos (~67% YoY revenue "
                    "growth in 2024). Megapack grid storage is the core product. "
                    "Musk's political involvement (DOGE, Trump relationship) "
                    "creates both opportunity and controversy for Tesla Energy."
                ),
                "political_connections": (
                    "Close relationship with Trump administration. Head of DOGE "
                    "(Department of Government Efficiency). Could influence energy "
                    "policy but also creates backlash (brand damage, government "
                    "contract scrutiny). IRA credits benefit Tesla Energy but "
                    "Musk's politics align with party that wants to repeal IRA."
                ),
                "confidence": "confirmed",
            },

            "key_products": [
                {
                    "product": "Megapack",
                    "type": "grid_scale_battery_storage",
                    "capacity_per_unit_mwh": 3.9,
                    "notes": (
                        "Utility-scale battery energy storage system (BESS). "
                        "Deployed at Lathrop, CA Megafactory and new Shanghai "
                        "Megafactory. Backlog exceeds production capacity. "
                        "Key product for grid stability as renewables penetration grows."
                    ),
                    "confidence": "confirmed",
                },
                {
                    "product": "Powerwall",
                    "type": "residential_battery",
                    "notes": "Home battery storage. Growing but smaller than Megapack.",
                    "confidence": "confirmed",
                },
                {
                    "product": "Solar Roof / Solar Panels",
                    "type": "residential_solar",
                    "notes": "Integrated solar shingles. Niche product. Not a major revenue driver.",
                    "confidence": "confirmed",
                },
            ],

            "competitive_position": {
                "grid_storage_market_share_pct": 20,
                "market_share_confidence": "estimated",
                "competitors": ["Fluence (Siemens/AES JV)", "BYD Energy Storage", "CATL", "Samsung SDI"],
                "advantage": "Vertical integration (cells from own/partner factories), software (Autobidder for grid trading), brand recognition.",
                "confidence": "estimated",
            },

            "trading_signals": [
                {
                    "signal": "Energy division becoming Tesla's growth story",
                    "direction": "bullish",
                    "timeframe": "2024-2028",
                    "description": "Energy revenue growing 67% YoY vs autos flat. If energy becomes >20% of revenue, TSLA valuation narrative shifts.",
                    "confidence": "derived",
                },
                {
                    "signal": "Musk political risk",
                    "direction": "risk",
                    "timeframe": "ongoing",
                    "description": "Musk's DOGE role and political polarization creates brand risk and potential government contract complications.",
                    "confidence": "confirmed",
                },
                {
                    "signal": "IRA paradox",
                    "direction": "risk",
                    "timeframe": "political_cycle",
                    "description": "Tesla Energy benefits from IRA credits, but Musk allies with politicians who want to repeal IRA. Unresolved tension.",
                    "confidence": "confirmed",
                },
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════
    # CROSS-CUTTING CONNECTIONS
    # ══════════════════════════════════════════════════════════════════

    "cross_connections": [
        {
            "type": "guyana_triangle",
            "entities": ["XOM", "CVX", "SAUDI_ARAMCO"],
            "description": (
                "Guyana's Stabroek Block is the most important new oil province. "
                "XOM operates (45%), Hess (30%) is being acquired by CVX, CNOOC (25%). "
                "ICC arbitration over preemptive rights. If Guyana production reaches "
                "1.5M bpd by 2030, it rivals OPEC members and pressures Saudi's "
                "market management strategy."
            ),
            "confidence": "confirmed",
        },
        {
            "type": "sanctions_shadow_market",
            "entities": ["ROSNEFT", "VITOL", "TRAFIGURA", "GUNVOR"],
            "description": (
                "Russian crude redirected through shadow fleet and trading intermediaries. "
                "Major traders publicly stepped back but indirect flows persist through "
                "Indian and Chinese refiners. Physical oil markets bifurcating into "
                "sanctioned and non-sanctioned tiers."
            ),
            "confidence": "estimated",
        },
        {
            "type": "lng_supercycle_beneficiaries",
            "entities": ["SHEL", "TTE", "SAUDI_ARAMCO", "ADNOC", "XOM"],
            "description": (
                "Post-Russia LNG demand from Europe + Asian growth. Shell is largest "
                "LNG trader. Qatar (TTE partner) expanding massively. US Gulf Coast "
                "LNG buildout (Golden Pass XOM/Qatar). $200B+ of LNG projects sanctioned."
            ),
            "confidence": "confirmed",
        },
        {
            "type": "ira_policy_nexus",
            "entities": ["NEE", "FSLR", "TSLA_ENERGY"],
            "description": (
                "All three heavily dependent on IRA tax credits. IRA repeal or "
                "modification would hit all simultaneously. Highly correlated "
                "to US political cycle."
            ),
            "confidence": "confirmed",
        },
        {
            "type": "opec_discipline_fragility",
            "entities": ["SAUDI_ARAMCO", "ADNOC", "IRAQ", "ROSNEFT"],
            "description": (
                "UAE wants higher quotas, Iraq chronically overproduces, Russia's "
                "compliance is unverifiable. Saudi bears disproportionate cut burden. "
                "Any OPEC+ fracture is bearish for oil prices but bullish for "
                "refiners and consumers."
            ),
            "confidence": "confirmed",
        },
        {
            "type": "transition_retreat_trade",
            "entities": ["SHEL", "BP", "XOM", "COP"],
            "description": (
                "Shell and BP retreating from renewables commitments. XOM and COP "
                "never seriously committed. European majors re-rating as oil/gas "
                "companies. Trade: long IOCs, short pure-play renewables if "
                "transition slows."
            ),
            "confidence": "inferred",
        },
        {
            "type": "commodity_trader_bribery_nexus",
            "entities": ["VITOL", "GLEN", "TRAFIGURA", "GUNVOR"],
            "description": (
                "All four major independent commodity traders have paid significant "
                "fines or been investigated for bribery in developing countries. "
                "Systemic issue in physical commodity trading. DOJ/SFO enforcement "
                "wave 2020-2024."
            ),
            "confidence": "confirmed",
        },
    ],
}
