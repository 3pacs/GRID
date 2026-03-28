"""
GRID Intelligence -- Global Lever Map: Hierarchical Model of World Economic Power.

The TEMPLATE that gets reused for every domain. Maps WHO controls WHAT in
the world economy, how they interact, and which levers are being pulled
right now.

Eight lever domains:
    1. monetary_policy    -- Who controls the price of money
    2. fiscal_policy      -- Who controls government spending
    3. regulation         -- Who controls the rules
    4. capital_allocation -- Who decides where money goes
    5. information        -- Who controls what people know
    6. technology         -- Who controls the infrastructure
    7. energy             -- Who controls energy
    8. trade              -- Who controls global trade

Each actor has:
    - name, entity, estimated influence (0-1)
    - what they control
    - who they report to
    - who influences them
    - known connections to actors in other lever categories

Key entry points:
    get_lever_hierarchy        -- full hierarchy (all 8 domains)
    get_lever_domain           -- single domain deep-dive
    trace_lever_chain          -- event -> chain of effects
    find_cross_domain_actors   -- actors appearing in 2+ domains
    generate_lever_report      -- narrative: who's pulling what lever right now

API: GET /api/v1/intelligence/levers
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# THE GLOBAL LEVER HIERARCHY
# ══════════════════════════════════════════════════════════════════════════

LEVER_HIERARCHY: dict[str, dict[str, Any]] = {
    # ──────────────────────────────────────────────────────────────────────
    # 1. MONETARY POLICY — Who Controls the Price of Money
    # ──────────────────────────────────────────────────────────────────────
    "monetary_policy": {
        "label": "Who Controls the Price of Money",
        "actors": {
            "tier_1": {
                "fed": {
                    "name": "Federal Reserve",
                    "entity": "US Federal Reserve System",
                    "influence": 1.0,
                    "controls": [
                        "us_interest_rates", "usd_supply", "bank_reserves",
                        "discount_window", "overnight_repo", "qe_qt",
                    ],
                    "reports_to": ["congress"],
                    "influenced_by": ["treasury", "bond_market", "white_house"],
                    "cross_domain": {
                        "regulation": "fed_supervision",
                        "capital_allocation": "reserve_requirements_shape_bank_lending",
                        "information": "fed_communications",
                    },
                    "key_personnel": [
                        {"name": "Jerome Powell", "title": "Chair", "influence": 0.99},
                        {"name": "John Williams", "title": "NY Fed President", "influence": 0.90},
                        {"name": "Philip Jefferson", "title": "Vice Chair", "influence": 0.88},
                        {"name": "Michael Barr", "title": "Vice Chair for Supervision", "influence": 0.85},
                        {"name": "Christopher Waller", "title": "Governor", "influence": 0.85},
                        {"name": "Michelle Bowman", "title": "Governor", "influence": 0.80},
                        {"name": "Lisa Cook", "title": "Governor", "influence": 0.78},
                        {"name": "Adriana Kugler", "title": "Governor", "influence": 0.76},
                    ],
                    "confidence": "hard_data",
                },
                "ecb": {
                    "name": "European Central Bank",
                    "entity": "ECB",
                    "influence": 0.85,
                    "controls": [
                        "euro_rates", "eur_supply", "tltro",
                        "asset_purchase_programme", "deposit_facility_rate",
                    ],
                    "reports_to": ["eu_parliament", "eu_council"],
                    "influenced_by": ["germany_finance", "france_finance", "bond_spreads"],
                    "cross_domain": {
                        "regulation": "ecb_banking_supervision",
                        "trade": "euro_fx_rate_affects_trade",
                    },
                    "key_personnel": [
                        {"name": "Christine Lagarde", "title": "President", "influence": 0.85},
                        {"name": "Luis de Guindos", "title": "Vice President", "influence": 0.70},
                        {"name": "Isabel Schnabel", "title": "Executive Board", "influence": 0.72},
                        {"name": "Philip Lane", "title": "Chief Economist", "influence": 0.74},
                    ],
                    "confidence": "hard_data",
                },
                "boj": {
                    "name": "Bank of Japan",
                    "entity": "BOJ",
                    "influence": 0.70,
                    "controls": [
                        "jpy_rates", "yield_curve_control", "etf_purchases",
                        "jgb_purchases", "negative_interest_rate_policy",
                    ],
                    "reports_to": ["japan_diet"],
                    "influenced_by": ["japan_mof", "yen_carry_trade", "us_treasury_yields"],
                    "cross_domain": {
                        "capital_allocation": "boj_etf_holdings_distort_equity_markets",
                        "trade": "yen_weakness_boosts_exports",
                    },
                    "key_personnel": [
                        {"name": "Kazuo Ueda", "title": "Governor", "influence": 0.70},
                        {"name": "Shinichi Uchida", "title": "Deputy Governor", "influence": 0.55},
                    ],
                    "confidence": "hard_data",
                },
                "pboc": {
                    "name": "People's Bank of China",
                    "entity": "PBOC",
                    "influence": 0.80,
                    "controls": [
                        "cny_rates", "rrr", "credit_impulse",
                        "mlf_rate", "lpr", "fx_reserves", "capital_controls",
                    ],
                    "reports_to": ["state_council"],
                    "influenced_by": ["ccp_politburo", "china_mof", "capital_flight_pressure"],
                    "cross_domain": {
                        "trade": "cny_management_is_trade_weapon",
                        "capital_allocation": "credit_impulse_drives_global_risk_appetite",
                    },
                    "key_personnel": [
                        {"name": "Pan Gongsheng", "title": "Governor", "influence": 0.80},
                    ],
                    "confidence": "hard_data",
                },
                "boe": {
                    "name": "Bank of England",
                    "entity": "BOE",
                    "influence": 0.55,
                    "controls": ["gbp_rates", "gbp_supply", "gilt_purchases"],
                    "reports_to": ["uk_parliament"],
                    "influenced_by": ["uk_treasury", "gilt_market"],
                    "cross_domain": {
                        "regulation": "boe_prudential_regulation",
                    },
                    "key_personnel": [
                        {"name": "Andrew Bailey", "title": "Governor", "influence": 0.55},
                    ],
                    "confidence": "hard_data",
                },
            },
            "tier_2": {
                "treasury": {
                    "name": "US Treasury",
                    "entity": "US Department of the Treasury",
                    "influence": 0.90,
                    "controls": ["tga_spending", "debt_issuance_mix", "sanctions"],
                    "reports_to": ["white_house", "congress"],
                    "influenced_by": ["bond_market", "deficit_projections", "political_cycle"],
                    "cross_domain": {
                        "fiscal_policy": "treasury_executes_fiscal_policy",
                        "trade": "treasury_sanctions_are_trade_weapons",
                        "regulation": "ofac_sanctions_enforcement",
                    },
                    "key_personnel": [
                        {"name": "Janet Yellen", "title": "Secretary", "influence": 0.90},
                        {"name": "Wally Adeyemo", "title": "Deputy Secretary", "influence": 0.75},
                    ],
                    "confidence": "hard_data",
                },
                "congress": {
                    "name": "US Congress",
                    "entity": "United States Congress",
                    "influence": 0.70,
                    "controls": ["fed_mandate", "debt_ceiling", "fed_appointments_confirmation"],
                    "reports_to": ["voters"],
                    "influenced_by": ["lobbyists", "donors", "media", "polls"],
                    "cross_domain": {
                        "fiscal_policy": "congress_controls_appropriations",
                        "regulation": "congress_writes_regulatory_laws",
                        "trade": "congress_approves_trade_agreements",
                    },
                    "key_personnel": [
                        {"name": "Senate Banking Committee Chair", "title": "Committee Chair", "influence": 0.65},
                        {"name": "House Financial Services Chair", "title": "Committee Chair", "influence": 0.60},
                    ],
                    "confidence": "public_record",
                },
                "bond_market": {
                    "name": "Bond Market (Aggregate)",
                    "entity": "Global Bond Markets",
                    "influence": 0.80,
                    "controls": ["long_rates", "term_premium", "credit_spreads"],
                    "reports_to": [],
                    "influenced_by": ["fed", "treasury_issuance", "inflation_expectations", "foreign_buyers"],
                    "cross_domain": {
                        "capital_allocation": "bond_yields_drive_asset_allocation",
                        "fiscal_policy": "bond_vigilantes_constrain_spending",
                    },
                    "key_personnel": [
                        {"name": "PIMCO", "title": "Largest active bond manager", "influence": 0.60},
                        {"name": "Japan GPIF", "title": "Largest pension fund", "influence": 0.55},
                        {"name": "China SAFE", "title": "Largest foreign holder", "influence": 0.65},
                    ],
                    "confidence": "derived",
                },
            },
            "tier_3": {
                "banks": {
                    "name": "Commercial Banks",
                    "entity": "US Banking System",
                    "influence": 0.55,
                    "controls": ["credit_creation", "lending_standards", "deposit_rates"],
                    "reports_to": ["fed_supervision", "occ", "fdic"],
                    "influenced_by": ["fed_rates", "reserve_requirements", "loan_demand"],
                    "cross_domain": {
                        "capital_allocation": "banks_are_primary_credit_allocators",
                        "regulation": "banks_subject_to_stress_tests",
                    },
                    "key_personnel": [
                        {"name": "Jamie Dimon", "title": "CEO, JPMorgan Chase", "influence": 0.65},
                        {"name": "David Solomon", "title": "CEO, Goldman Sachs", "influence": 0.55},
                        {"name": "Jane Fraser", "title": "CEO, Citigroup", "influence": 0.50},
                        {"name": "Brian Moynihan", "title": "CEO, Bank of America", "influence": 0.50},
                    ],
                    "confidence": "hard_data",
                },
                "market_makers": {
                    "name": "Market Makers & Dealers",
                    "entity": "Primary Dealers + Electronic Market Makers",
                    "influence": 0.45,
                    "controls": ["liquidity", "bid_ask_spreads", "repo_rates"],
                    "reports_to": ["sec", "finra", "fed"],
                    "influenced_by": ["volatility", "inventory_risk", "regulation"],
                    "cross_domain": {
                        "regulation": "subject_to_volcker_rule",
                        "information": "market_makers_see_order_flow_first",
                    },
                    "key_personnel": [
                        {"name": "Citadel Securities", "title": "Dominant equity MM", "influence": 0.45},
                        {"name": "Jane Street", "title": "Major ETF/options MM", "influence": 0.40},
                        {"name": "Virtu Financial", "title": "Electronic MM", "influence": 0.35},
                    ],
                    "confidence": "derived",
                },
            },
        },
        "transmission": (
            "rate_decision -> bank_reserves -> lending_standards -> credit_creation "
            "-> asset_prices -> wealth_effect -> consumer_spending -> economy"
        ),
    },

    # ──────────────────────────────────────────────────────────────────────
    # 2. FISCAL POLICY — Who Controls Government Spending
    # ──────────────────────────────────────────────────────────────────────
    "fiscal_policy": {
        "label": "Who Controls Government Spending",
        "actors": {
            "tier_1": {
                "congress": {
                    "name": "US Congress",
                    "entity": "United States Congress",
                    "influence": 0.95,
                    "controls": [
                        "appropriations", "tax_policy", "debt_ceiling",
                        "entitlement_programs", "defense_budget",
                    ],
                    "reports_to": ["voters"],
                    "influenced_by": ["lobbyists", "donors", "media", "polls", "party_leadership"],
                    "cross_domain": {
                        "monetary_policy": "congress_sets_fed_mandate",
                        "regulation": "congress_writes_laws",
                        "trade": "congress_approves_trade_deals",
                    },
                    "key_personnel": [
                        {"name": "Speaker of the House", "title": "Speaker", "influence": 0.90},
                        {"name": "Senate Majority Leader", "title": "Leader", "influence": 0.88},
                        {"name": "House Appropriations Chair", "title": "Chair", "influence": 0.80},
                        {"name": "Senate Finance Chair", "title": "Chair", "influence": 0.78},
                    ],
                    "confidence": "public_record",
                },
                "white_house": {
                    "name": "White House / Executive Branch",
                    "entity": "Office of the President",
                    "influence": 0.90,
                    "controls": [
                        "budget_proposal", "executive_orders", "emergency_spending",
                        "veto_power", "omb_priorities",
                    ],
                    "reports_to": ["voters", "constitution"],
                    "influenced_by": ["donors", "party", "polls", "national_security_council"],
                    "cross_domain": {
                        "monetary_policy": "president_appoints_fed_chair",
                        "regulation": "president_appoints_agency_heads",
                        "trade": "president_sets_trade_policy",
                        "energy": "president_controls_spr_releases",
                    },
                    "key_personnel": [
                        {"name": "President", "title": "President", "influence": 0.95},
                        {"name": "OMB Director", "title": "Director", "influence": 0.70},
                        {"name": "Chief of Staff", "title": "Chief of Staff", "influence": 0.65},
                        {"name": "NEC Director", "title": "Director", "influence": 0.60},
                    ],
                    "confidence": "public_record",
                },
                "treasury_fiscal": {
                    "name": "US Treasury (Fiscal Operations)",
                    "entity": "US Department of the Treasury",
                    "influence": 0.85,
                    "controls": [
                        "tga_account", "debt_issuance_timing", "tax_collection",
                        "irs_enforcement", "sanctions_enforcement",
                    ],
                    "reports_to": ["white_house", "congress"],
                    "influenced_by": ["bond_market", "tax_receipts", "deficit_trajectory"],
                    "cross_domain": {
                        "monetary_policy": "tga_flows_affect_reserves",
                        "trade": "treasury_sanctions",
                    },
                    "key_personnel": [
                        {"name": "Janet Yellen", "title": "Secretary", "influence": 0.90},
                        {"name": "IRS Commissioner", "title": "Commissioner", "influence": 0.55},
                    ],
                    "confidence": "hard_data",
                },
            },
            "tier_2": {
                "lobbyists": {
                    "name": "Lobbying Industry",
                    "entity": "K Street / Registered Lobbyists",
                    "influence": 0.65,
                    "controls": ["legislative_language", "committee_access", "earmark_requests"],
                    "reports_to": ["corporate_clients", "trade_associations"],
                    "influenced_by": ["corporate_interests", "campaign_finance_rules"],
                    "cross_domain": {
                        "regulation": "lobbyists_shape_regulatory_outcomes",
                        "energy": "oil_gas_lobbying_shapes_energy_policy",
                        "technology": "tech_lobbying_shapes_antitrust",
                    },
                    "key_personnel": [
                        {"name": "US Chamber of Commerce", "title": "Largest business lobby", "influence": 0.60},
                        {"name": "AIPAC", "title": "Foreign policy lobby", "influence": 0.55},
                        {"name": "NRA", "title": "Gun lobby", "influence": 0.45},
                        {"name": "PhRMA", "title": "Pharma lobby", "influence": 0.55},
                    ],
                    "confidence": "public_record",
                },
                "defense_contractors": {
                    "name": "Defense Industry",
                    "entity": "Major Defense Contractors",
                    "influence": 0.60,
                    "controls": ["weapons_programs", "base_locations", "military_tech"],
                    "reports_to": ["dod", "congress"],
                    "influenced_by": ["geopolitical_threats", "budget_cycles", "procurement_rules"],
                    "cross_domain": {
                        "technology": "defense_r_and_d_shapes_tech_industry",
                        "trade": "arms_exports",
                    },
                    "key_personnel": [
                        {"name": "Lockheed Martin", "title": "Largest defense contractor", "influence": 0.55},
                        {"name": "Raytheon/RTX", "title": "Missiles & defense", "influence": 0.50},
                        {"name": "Northrop Grumman", "title": "Stealth & space", "influence": 0.48},
                        {"name": "Boeing Defense", "title": "Aircraft & space", "influence": 0.48},
                    ],
                    "confidence": "public_record",
                },
                "donors": {
                    "name": "Major Political Donors",
                    "entity": "Super PACs / Bundlers / Dark Money",
                    "influence": 0.55,
                    "controls": ["campaign_funding", "super_pac_spending", "issue_ads"],
                    "reports_to": [],
                    "influenced_by": ["tax_policy", "regulatory_environment", "personal_wealth"],
                    "cross_domain": {
                        "regulation": "donors_influence_regulatory_appointments",
                        "technology": "tech_billionaires_fund_campaigns",
                    },
                    "key_personnel": [
                        {"name": "Koch Network", "title": "Conservative mega-donor", "influence": 0.50},
                        {"name": "Bloomberg LP", "title": "Bloomberg philanthropies", "influence": 0.45},
                        {"name": "George Soros / Open Society", "title": "Progressive donor", "influence": 0.40},
                    ],
                    "confidence": "public_record",
                },
            },
            "tier_3": {
                "state_governments": {
                    "name": "State Governments",
                    "entity": "50 US State Governments",
                    "influence": 0.40,
                    "controls": ["state_budgets", "local_taxes", "infrastructure_spending", "medicaid_admin"],
                    "reports_to": ["state_voters", "state_legislatures"],
                    "influenced_by": ["federal_grants", "unfunded_mandates", "state_tax_revenue"],
                    "cross_domain": {
                        "regulation": "state_regulators_add_layer",
                        "energy": "state_renewable_mandates",
                    },
                    "confidence": "public_record",
                },
                "municipalities": {
                    "name": "Municipal Governments",
                    "entity": "Cities, Counties, Special Districts",
                    "influence": 0.25,
                    "controls": ["property_tax", "zoning", "local_bonds", "public_works"],
                    "reports_to": ["local_voters", "state_governments"],
                    "influenced_by": ["property_values", "migration_patterns", "federal_aid"],
                    "cross_domain": {
                        "capital_allocation": "muni_bonds_are_major_asset_class",
                    },
                    "confidence": "public_record",
                },
            },
        },
        "transmission": (
            "appropriation -> agency_budget -> contracts -> companies -> jobs -> "
            "consumer_spending -> tax_revenue -> deficit -> bond_issuance -> rates"
        ),
    },

    # ──────────────────────────────────────────────────────────────────────
    # 3. REGULATION — Who Controls the Rules
    # ──────────────────────────────────────────────────────────────────────
    "regulation": {
        "label": "Who Controls the Rules",
        "actors": {
            "tier_1": {
                "sec": {
                    "name": "Securities and Exchange Commission",
                    "entity": "SEC",
                    "influence": 0.85,
                    "controls": [
                        "securities_law_enforcement", "ipo_approvals", "disclosure_rules",
                        "market_structure_rules", "short_selling_rules", "crypto_enforcement",
                    ],
                    "reports_to": ["congress", "white_house"],
                    "influenced_by": ["industry_lobbyists", "congressional_committees", "courts"],
                    "cross_domain": {
                        "capital_allocation": "sec_rules_determine_who_can_invest_in_what",
                        "information": "sec_mandates_corporate_disclosure",
                    },
                    "key_personnel": [
                        {"name": "Gary Gensler", "title": "Chair", "influence": 0.80},
                    ],
                    "confidence": "hard_data",
                },
                "cftc": {
                    "name": "Commodity Futures Trading Commission",
                    "entity": "CFTC",
                    "influence": 0.60,
                    "controls": [
                        "futures_regulation", "derivatives_oversight", "position_limits",
                        "swaps_clearing", "commodity_speculation_rules",
                    ],
                    "reports_to": ["congress"],
                    "influenced_by": ["industry_lobbyists", "agricultural_interests"],
                    "cross_domain": {
                        "energy": "cftc_regulates_oil_futures",
                        "capital_allocation": "cftc_position_limits_constrain_speculation",
                    },
                    "key_personnel": [
                        {"name": "CFTC Chair", "title": "Chair", "influence": 0.55},
                    ],
                    "confidence": "hard_data",
                },
                "fed_supervision": {
                    "name": "Federal Reserve Supervision",
                    "entity": "Fed Board of Governors — Supervision & Regulation",
                    "influence": 0.80,
                    "controls": [
                        "bank_stress_tests", "capital_requirements", "slr",
                        "bank_merger_approvals", "systemic_risk_designation",
                    ],
                    "reports_to": ["congress"],
                    "influenced_by": ["bank_lobby", "financial_stability_concerns"],
                    "cross_domain": {
                        "monetary_policy": "supervision_and_monetary_policy_interact",
                        "capital_allocation": "capital_rules_shape_bank_lending",
                    },
                    "key_personnel": [
                        {"name": "Michael Barr", "title": "Vice Chair for Supervision", "influence": 0.80},
                    ],
                    "confidence": "hard_data",
                },
                "doj": {
                    "name": "Department of Justice",
                    "entity": "DOJ — Antitrust Division + Criminal",
                    "influence": 0.75,
                    "controls": [
                        "antitrust_enforcement", "criminal_fraud", "insider_trading_prosecution",
                        "merger_challenges", "corporate_settlements",
                    ],
                    "reports_to": ["white_house", "congress"],
                    "influenced_by": ["political_priorities", "public_pressure", "courts"],
                    "cross_domain": {
                        "technology": "doj_antitrust_shapes_tech_industry",
                        "capital_allocation": "merger_enforcement_affects_deal_flow",
                    },
                    "key_personnel": [
                        {"name": "Attorney General", "title": "AG", "influence": 0.70},
                        {"name": "AAG Antitrust", "title": "Assistant AG", "influence": 0.60},
                    ],
                    "confidence": "hard_data",
                },
            },
            "tier_2": {
                "congressional_committees": {
                    "name": "Congressional Oversight Committees",
                    "entity": "Senate Banking, House Financial Services, Judiciary",
                    "influence": 0.65,
                    "controls": ["hearings", "subpoena_power", "legislation_drafting", "agency_budgets"],
                    "reports_to": ["voters"],
                    "influenced_by": ["lobbyists", "donors", "media_pressure"],
                    "cross_domain": {
                        "fiscal_policy": "committees_control_agency_funding",
                        "monetary_policy": "banking_committees_oversee_fed",
                    },
                    "confidence": "public_record",
                },
                "industry_lobbyists": {
                    "name": "Financial Industry Lobbyists",
                    "entity": "SIFMA, ABA, Chamber of Commerce, etc.",
                    "influence": 0.60,
                    "controls": ["comment_letters", "revolving_door", "campaign_contributions"],
                    "reports_to": ["financial_institutions"],
                    "influenced_by": ["regulatory_proposals", "enforcement_actions"],
                    "cross_domain": {
                        "fiscal_policy": "lobbyists_shape_tax_code",
                        "capital_allocation": "lobby_for_favorable_investment_rules",
                    },
                    "confidence": "public_record",
                },
            },
            "tier_3": {
                "compliance_industry": {
                    "name": "Compliance & Risk Industry",
                    "entity": "Big 4 Accounting, Compliance Vendors",
                    "influence": 0.30,
                    "controls": ["audit_standards", "compliance_frameworks", "risk_models"],
                    "reports_to": ["regulators", "corporate_boards"],
                    "influenced_by": ["regulatory_changes", "enforcement_trends"],
                    "cross_domain": {
                        "information": "auditors_validate_corporate_disclosures",
                    },
                    "key_personnel": [
                        {"name": "Deloitte", "title": "Big 4", "influence": 0.28},
                        {"name": "PwC", "title": "Big 4", "influence": 0.28},
                        {"name": "EY", "title": "Big 4", "influence": 0.27},
                        {"name": "KPMG", "title": "Big 4", "influence": 0.27},
                    ],
                    "confidence": "derived",
                },
                "law_firms": {
                    "name": "Major Securities Law Firms",
                    "entity": "Sullivan & Cromwell, Skadden, Wachtell, etc.",
                    "influence": 0.35,
                    "controls": ["legal_precedent", "sec_comment_letters", "corporate_defense"],
                    "reports_to": ["corporate_clients"],
                    "influenced_by": ["case_law", "regulatory_shifts"],
                    "cross_domain": {
                        "capital_allocation": "m_and_a_lawyers_facilitate_deals",
                    },
                    "confidence": "derived",
                },
                "consultants": {
                    "name": "Management Consultants",
                    "entity": "McKinsey, BCG, Bain",
                    "influence": 0.25,
                    "controls": ["strategy_recommendations", "restructuring_plans", "government_contracts"],
                    "reports_to": ["corporate_clients", "government_agencies"],
                    "influenced_by": ["fee_incentives", "industry_trends"],
                    "cross_domain": {
                        "fiscal_policy": "consultants_advise_government_agencies",
                        "technology": "consultants_drive_digital_transformation",
                    },
                    "confidence": "derived",
                },
            },
        },
        "transmission": (
            "proposal -> comment_period -> final_rule -> compliance_deadline -> "
            "behavior_change -> market_structure_shift -> price_impact"
        ),
    },

    # ──────────────────────────────────────────────────────────────────────
    # 4. CAPITAL ALLOCATION — Who Decides Where Money Goes
    # ──────────────────────────────────────────────────────────────────────
    "capital_allocation": {
        "label": "Who Decides Where Money Goes",
        "actors": {
            "tier_1": {
                "blackrock": {
                    "name": "BlackRock",
                    "entity": "BlackRock, Inc.",
                    "influence": 0.90,
                    "controls": [
                        "index_composition", "etf_flows", "proxy_voting",
                        "corporate_governance_pressure", "esg_standards",
                    ],
                    "reports_to": ["shareholders", "sec"],
                    "influenced_by": ["index_rules", "client_flows", "regulatory_environment"],
                    "cross_domain": {
                        "regulation": "blackrock_lobbies_on_esg_and_proxy_rules",
                        "information": "blackrock_investment_institute_shapes_narrative",
                        "energy": "blackrock_esg_pressure_on_fossil_fuels",
                    },
                    "key_personnel": [
                        {"name": "Larry Fink", "title": "CEO", "influence": 0.85},
                        {"name": "Rob Kapito", "title": "President", "influence": 0.60},
                    ],
                    "aum": 10_500_000_000_000,
                    "confidence": "hard_data",
                },
                "vanguard": {
                    "name": "Vanguard Group",
                    "entity": "The Vanguard Group",
                    "influence": 0.85,
                    "controls": [
                        "index_fund_flows", "proxy_voting", "fee_compression",
                        "passive_ownership_concentration",
                    ],
                    "reports_to": ["fund_shareholders"],
                    "influenced_by": ["index_rules", "client_flows"],
                    "cross_domain": {
                        "regulation": "vanguard_proxy_voting_shapes_governance",
                    },
                    "aum": 8_600_000_000_000,
                    "confidence": "hard_data",
                },
                "state_street": {
                    "name": "State Street Global Advisors",
                    "entity": "State Street Corporation",
                    "influence": 0.70,
                    "controls": [
                        "spy_etf_flows", "proxy_voting", "custody_services",
                        "index_rebalancing",
                    ],
                    "reports_to": ["shareholders", "sec"],
                    "influenced_by": ["index_rules", "client_flows"],
                    "cross_domain": {
                        "regulation": "state_street_custody_is_systemic",
                    },
                    "aum": 4_100_000_000_000,
                    "confidence": "hard_data",
                },
            },
            "tier_2": {
                "pension_cios": {
                    "name": "Pension Fund CIOs",
                    "entity": "CalPERS, CalSTRS, NY Common, OTPP, etc.",
                    "influence": 0.70,
                    "controls": [
                        "asset_allocation_targets", "alternatives_allocation",
                        "private_credit_commitments", "real_estate_allocation",
                    ],
                    "reports_to": ["pension_boards", "state_governments", "beneficiaries"],
                    "influenced_by": ["actuarial_assumptions", "funded_status", "consultants"],
                    "cross_domain": {
                        "fiscal_policy": "pension_underfunding_is_fiscal_time_bomb",
                    },
                    "key_personnel": [
                        {"name": "CalPERS CIO", "title": "CIO", "influence": 0.60},
                        {"name": "CalSTRS CIO", "title": "CIO", "influence": 0.55},
                        {"name": "Norway GPFG", "title": "World's largest SWF", "influence": 0.65},
                    ],
                    "aum": 5_000_000_000_000,  # combined major pensions
                    "confidence": "public_record",
                },
                "sovereign_wealth": {
                    "name": "Sovereign Wealth Funds",
                    "entity": "ADIA, GIC, PIF, CIC, QIA, KIA, etc.",
                    "influence": 0.75,
                    "controls": [
                        "sovereign_capital_deployment", "strategic_investments",
                        "fx_reserve_management", "infrastructure_investment",
                    ],
                    "reports_to": ["sovereign_governments"],
                    "influenced_by": ["oil_revenue", "geopolitics", "diversification_mandate"],
                    "cross_domain": {
                        "energy": "petro_state_swfs_funded_by_oil",
                        "trade": "swf_investments_are_geopolitical",
                        "technology": "swfs_invest_heavily_in_tech",
                    },
                    "key_personnel": [
                        {"name": "ADIA (Abu Dhabi)", "title": "~$990B AUM", "influence": 0.70},
                        {"name": "GIC (Singapore)", "title": "~$770B AUM", "influence": 0.65},
                        {"name": "PIF (Saudi Arabia)", "title": "~$930B AUM", "influence": 0.70},
                        {"name": "CIC (China)", "title": "~$1.3T AUM", "influence": 0.72},
                    ],
                    "aum": 12_000_000_000_000,  # combined
                    "confidence": "estimated",
                },
                "endowments": {
                    "name": "University Endowments",
                    "entity": "Harvard, Yale, Stanford, MIT, Princeton, etc.",
                    "influence": 0.40,
                    "controls": ["alternatives_pioneering", "venture_allocation", "timber_real_assets"],
                    "reports_to": ["university_boards"],
                    "influenced_by": ["yale_model", "consultant_recommendations", "peer_benchmarking"],
                    "cross_domain": {
                        "technology": "endowments_are_major_vc_lps",
                    },
                    "key_personnel": [
                        {"name": "Harvard Management Co.", "title": "~$50B", "influence": 0.35},
                        {"name": "Yale Investments Office", "title": "~$41B", "influence": 0.38},
                    ],
                    "aum": 900_000_000_000,  # combined top 100
                    "confidence": "public_record",
                },
            },
            "tier_3": {
                "hedge_funds": {
                    "name": "Hedge Funds",
                    "entity": "Bridgewater, Citadel, Millennium, DE Shaw, etc.",
                    "influence": 0.65,
                    "controls": [
                        "price_discovery", "short_selling", "event_driven_catalysts",
                        "volatility_trading", "macro_bets",
                    ],
                    "reports_to": ["lps", "sec"],
                    "influenced_by": ["alpha_decay", "redemptions", "margin_requirements"],
                    "cross_domain": {
                        "information": "hedge_funds_are_sophisticated_info_consumers",
                        "regulation": "subject_to_13f_disclosure",
                    },
                    "key_personnel": [
                        {"name": "Ken Griffin", "title": "Citadel CEO", "influence": 0.60},
                        {"name": "Ray Dalio", "title": "Bridgewater founder", "influence": 0.55},
                        {"name": "Izzy Englander", "title": "Millennium CEO", "influence": 0.50},
                        {"name": "David Shaw", "title": "DE Shaw founder", "influence": 0.45},
                    ],
                    "aum": 4_500_000_000_000,
                    "confidence": "derived",
                },
                "private_equity": {
                    "name": "Private Equity",
                    "entity": "KKR, Apollo, Blackstone PE, Carlyle, TPG, etc.",
                    "influence": 0.70,
                    "controls": [
                        "lbo_activity", "corporate_restructuring", "take_private",
                        "portfolio_company_operations", "dividend_recaps",
                    ],
                    "reports_to": ["lps", "sec"],
                    "influenced_by": ["interest_rates", "credit_spreads", "fundraising_cycle"],
                    "cross_domain": {
                        "monetary_policy": "pe_leveraged_returns_depend_on_low_rates",
                        "regulation": "pe_lobbies_against_carried_interest_reform",
                        "fiscal_policy": "pe_benefits_from_carried_interest_tax_treatment",
                    },
                    "key_personnel": [
                        {"name": "Steve Schwarzman", "title": "Blackstone CEO", "influence": 0.65},
                        {"name": "Marc Rowan", "title": "Apollo CEO", "influence": 0.60},
                        {"name": "Henry Kravis", "title": "KKR Co-Founder", "influence": 0.55},
                        {"name": "David Rubenstein", "title": "Carlyle Co-Founder", "influence": 0.50},
                    ],
                    "aum": 8_000_000_000_000,
                    "confidence": "derived",
                },
                "venture_capital": {
                    "name": "Venture Capital",
                    "entity": "a16z, Sequoia, Accel, Benchmark, etc.",
                    "influence": 0.50,
                    "controls": [
                        "startup_funding", "tech_company_formation",
                        "ipo_pipeline", "innovation_direction",
                    ],
                    "reports_to": ["lps"],
                    "influenced_by": ["ipo_market", "interest_rates", "tech_trends"],
                    "cross_domain": {
                        "technology": "vc_shapes_which_technologies_get_built",
                        "information": "vc_narrative_shapes_tech_media",
                    },
                    "key_personnel": [
                        {"name": "Marc Andreessen", "title": "a16z co-founder", "influence": 0.50},
                        {"name": "Sequoia Capital", "title": "Premier VC firm", "influence": 0.48},
                        {"name": "Masayoshi Son", "title": "SoftBank Vision Fund", "influence": 0.45},
                    ],
                    "confidence": "derived",
                },
            },
            "tier_4": {
                "retail": {
                    "name": "Retail Investors",
                    "entity": "Individual Investors (Aggregate)",
                    "influence": 0.25,
                    "controls": ["meme_stock_squeezes", "options_gamma_ramps", "etf_inflows"],
                    "reports_to": [],
                    "influenced_by": [
                        "social_media", "cnbc", "fomo", "loss_aversion",
                        "robinhood_gamification",
                    ],
                    "cross_domain": {
                        "information": "retail_is_most_influenced_by_info_layer",
                    },
                    "confidence": "estimated",
                },
                "robinhood_traders": {
                    "name": "Zero-Commission App Traders",
                    "entity": "Robinhood, Webull, Public.com, etc.",
                    "influence": 0.15,
                    "controls": ["options_volume_spikes", "meme_momentum", "small_cap_liquidity"],
                    "reports_to": [],
                    "influenced_by": [
                        "tiktok_finfluencers", "reddit_wsb", "push_notifications",
                        "gamification_mechanics",
                    ],
                    "cross_domain": {
                        "information": "app_traders_are_end_of_info_chain",
                        "regulation": "pfof_debate",
                    },
                    "confidence": "estimated",
                },
            },
        },
        "transmission": (
            "index_rebalance -> passive_flows -> price_impact -> active_rebalance -> "
            "momentum -> retail_follows -> more_momentum -> mean_reversion"
        ),
    },

    # ──────────────────────────────────────────────────────────────────────
    # 5. INFORMATION — Who Controls What People Know
    # ──────────────────────────────────────────────────────────────────────
    "information": {
        "label": "Who Controls What People Know",
        "actors": {
            "tier_1": {
                "bloomberg_terminal": {
                    "name": "Bloomberg Terminal",
                    "entity": "Bloomberg LP",
                    "influence": 0.90,
                    "controls": [
                        "real_time_data", "news_wire", "chat_network",
                        "analytics_platform", "index_calculations",
                    ],
                    "reports_to": ["bloomberg_lp_ownership"],
                    "influenced_by": ["data_providers", "regulatory_filings"],
                    "cross_domain": {
                        "capital_allocation": "bloomberg_is_infrastructure_for_allocators",
                        "regulation": "bloomberg_data_used_for_compliance",
                    },
                    "key_personnel": [
                        {"name": "Michael Bloomberg", "title": "Founder", "influence": 0.75},
                    ],
                    "confidence": "hard_data",
                },
                "reuters": {
                    "name": "Reuters / LSEG",
                    "entity": "London Stock Exchange Group",
                    "influence": 0.75,
                    "controls": [
                        "news_wire", "eikon_terminal", "fx_benchmark_rates",
                        "clearing_data", "index_licensing",
                    ],
                    "reports_to": ["lseg_shareholders"],
                    "influenced_by": ["data_licensing_revenue", "competitive_pressure"],
                    "cross_domain": {
                        "trade": "reuters_fx_benchmarks_used_globally",
                    },
                    "confidence": "hard_data",
                },
                "fed_communications": {
                    "name": "Federal Reserve Communications",
                    "entity": "FOMC Statements, Minutes, Speeches, Dot Plot",
                    "influence": 0.95,
                    "controls": [
                        "forward_guidance", "rate_expectations", "risk_sentiment",
                        "inflation_narrative",
                    ],
                    "reports_to": ["fed_board"],
                    "influenced_by": ["economic_data", "financial_conditions"],
                    "cross_domain": {
                        "monetary_policy": "communications_IS_monetary_policy",
                    },
                    "confidence": "hard_data",
                },
            },
            "tier_2": {
                "cnbc": {
                    "name": "CNBC",
                    "entity": "NBCUniversal / Comcast",
                    "influence": 0.50,
                    "controls": ["retail_narrative", "market_sentiment", "ceo_interview_platform"],
                    "reports_to": ["comcast_ownership"],
                    "influenced_by": ["ratings", "advertising_revenue", "access_journalism"],
                    "cross_domain": {
                        "capital_allocation": "cnbc_moves_retail_flows",
                    },
                    "key_personnel": [
                        {"name": "Jim Cramer", "title": "Mad Money Host", "influence": 0.35},
                    ],
                    "confidence": "derived",
                },
                "wsj": {
                    "name": "Wall Street Journal",
                    "entity": "Dow Jones / News Corp",
                    "influence": 0.65,
                    "controls": ["policy_narrative", "corporate_investigations", "editorial_influence"],
                    "reports_to": ["news_corp_ownership"],
                    "influenced_by": ["sources", "editorial_board"],
                    "cross_domain": {
                        "regulation": "wsj_investigations_trigger_enforcement",
                        "fiscal_policy": "wsj_editorial_shapes_fiscal_debate",
                    },
                    "key_personnel": [
                        {"name": "Rupert Murdoch family", "title": "News Corp controller", "influence": 0.50},
                    ],
                    "confidence": "derived",
                },
                "ft": {
                    "name": "Financial Times",
                    "entity": "Nikkei Inc.",
                    "influence": 0.60,
                    "controls": ["global_financial_narrative", "european_policy_coverage"],
                    "reports_to": ["nikkei_ownership"],
                    "influenced_by": ["sources", "editorial_standards"],
                    "cross_domain": {
                        "trade": "ft_shapes_global_trade_narrative",
                    },
                    "confidence": "derived",
                },
                "analyst_research": {
                    "name": "Sell-Side Research",
                    "entity": "Goldman, Morgan Stanley, JPM Research, etc.",
                    "influence": 0.55,
                    "controls": [
                        "price_targets", "earnings_estimates", "sector_calls",
                        "thematic_research",
                    ],
                    "reports_to": ["investment_banks"],
                    "influenced_by": [
                        "banking_relationships", "commission_revenue",
                        "regulatory_separation_rules",
                    ],
                    "cross_domain": {
                        "capital_allocation": "research_informs_allocation_decisions",
                    },
                    "confidence": "derived",
                },
            },
            "tier_3": {
                "social_media": {
                    "name": "Social Media Platforms",
                    "entity": "X/Twitter, YouTube, TikTok",
                    "influence": 0.40,
                    "controls": [
                        "viral_narratives", "finfluencer_reach", "algorithmic_amplification",
                    ],
                    "reports_to": ["platform_owners"],
                    "influenced_by": ["engagement_algorithms", "advertising_model"],
                    "cross_domain": {
                        "capital_allocation": "social_media_drives_meme_stocks",
                        "regulation": "platform_regulation_debate",
                    },
                    "confidence": "estimated",
                },
                "fintwit": {
                    "name": "FinTwit / Finance Twitter",
                    "entity": "Financial Twitter Community",
                    "influence": 0.30,
                    "controls": ["real_time_market_commentary", "trade_ideas", "narrative_formation"],
                    "reports_to": [],
                    "influenced_by": ["market_moves", "breaking_news", "ego"],
                    "cross_domain": {
                        "capital_allocation": "fintwit_influences_retail_allocation",
                    },
                    "confidence": "estimated",
                },
                "reddit": {
                    "name": "Reddit Finance Communities",
                    "entity": "r/wallstreetbets, r/investing, r/stocks",
                    "influence": 0.25,
                    "controls": ["meme_stock_coordination", "options_gamma_squeezes", "dd_research"],
                    "reports_to": [],
                    "influenced_by": ["market_volatility", "loss_porn", "yolo_culture"],
                    "cross_domain": {
                        "capital_allocation": "wsb_proved_retail_can_move_markets",
                    },
                    "confidence": "estimated",
                },
                "podcasts": {
                    "name": "Finance Podcasts",
                    "entity": "All-In, Macro Voices, Odd Lots, etc.",
                    "influence": 0.20,
                    "controls": ["long_form_narrative", "thesis_propagation"],
                    "reports_to": [],
                    "influenced_by": ["sponsorships", "guest_selection", "host_bias"],
                    "cross_domain": {
                        "capital_allocation": "podcast_theses_drive_retail_allocation",
                    },
                    "confidence": "estimated",
                },
            },
        },
        "transmission": (
            "event -> insiders_know (T+0) -> bloomberg_wire (T+seconds) -> "
            "institutional_reaction (T+minutes) -> media_reports (T+hours) -> "
            "retail_reacts (T+days) -> price_fully_adjusted"
        ),
    },

    # ──────────────────────────────────────────────────────────────────────
    # 6. TECHNOLOGY — Who Controls the Infrastructure
    # ──────────────────────────────────────────────────────────────────────
    "technology": {
        "label": "Who Controls the Infrastructure",
        "actors": {
            "tier_1": {
                "cloud_providers": {
                    "name": "Hyperscale Cloud Providers",
                    "entity": "AWS, Azure, GCP",
                    "influence": 0.85,
                    "controls": [
                        "compute_infrastructure", "cloud_pricing", "data_sovereignty",
                        "ai_training_infrastructure", "enterprise_lock_in",
                    ],
                    "reports_to": ["shareholders"],
                    "influenced_by": ["enterprise_demand", "ai_capex_cycle", "regulation"],
                    "cross_domain": {
                        "capital_allocation": "cloud_capex_drives_semis_and_infra",
                        "regulation": "cloud_concentration_antitrust_risk",
                    },
                    "key_personnel": [
                        {"name": "Andy Jassy", "title": "Amazon CEO (AWS parent)", "influence": 0.75},
                        {"name": "Satya Nadella", "title": "Microsoft CEO (Azure parent)", "influence": 0.80},
                        {"name": "Sundar Pichai", "title": "Alphabet CEO (GCP parent)", "influence": 0.72},
                    ],
                    "confidence": "hard_data",
                },
                "chip_makers": {
                    "name": "Semiconductor Companies",
                    "entity": "NVIDIA, TSMC, ASML, Intel, AMD",
                    "influence": 0.80,
                    "controls": [
                        "ai_chip_supply", "process_node_advancement", "chip_pricing",
                        "foundry_capacity", "lithography_monopoly",
                    ],
                    "reports_to": ["shareholders"],
                    "influenced_by": ["ai_demand", "geopolitics", "export_controls"],
                    "cross_domain": {
                        "trade": "chip_export_controls_are_geopolitical_weapon",
                        "capital_allocation": "ai_capex_drives_semi_valuations",
                    },
                    "key_personnel": [
                        {"name": "Jensen Huang", "title": "NVIDIA CEO", "influence": 0.80},
                        {"name": "C.C. Wei", "title": "TSMC CEO", "influence": 0.75},
                        {"name": "Peter Wennink", "title": "ASML CEO", "influence": 0.65},
                        {"name": "Lisa Su", "title": "AMD CEO", "influence": 0.60},
                    ],
                    "confidence": "hard_data",
                },
                "network_infra": {
                    "name": "Network Infrastructure",
                    "entity": "Submarine Cables, CDNs, Exchanges",
                    "influence": 0.50,
                    "controls": [
                        "internet_backbone", "latency", "data_routing",
                        "exchange_connectivity", "cdn_distribution",
                    ],
                    "reports_to": ["shareholders", "regulators"],
                    "influenced_by": ["bandwidth_demand", "geopolitical_cable_routes"],
                    "cross_domain": {
                        "trade": "submarine_cables_enable_global_commerce",
                        "information": "network_infra_determines_info_speed",
                    },
                    "confidence": "derived",
                },
            },
            "tier_2": {
                "software_platforms": {
                    "name": "Software Platforms",
                    "entity": "Microsoft, Salesforce, Oracle, SAP",
                    "influence": 0.65,
                    "controls": [
                        "enterprise_workflows", "data_formats", "ecosystem_lock_in",
                        "platform_pricing",
                    ],
                    "reports_to": ["shareholders"],
                    "influenced_by": ["enterprise_budgets", "ai_disruption", "competition"],
                    "cross_domain": {
                        "regulation": "platform_antitrust_scrutiny",
                        "information": "software_platforms_control_data_access",
                    },
                    "confidence": "hard_data",
                },
                "ai_companies": {
                    "name": "AI Companies",
                    "entity": "OpenAI, Anthropic, Google DeepMind, Meta AI, xAI",
                    "influence": 0.70,
                    "controls": [
                        "ai_model_capabilities", "ai_safety_norms", "ai_pricing",
                        "knowledge_synthesis", "automation_frontier",
                    ],
                    "reports_to": ["investors", "boards"],
                    "influenced_by": ["compute_costs", "talent_competition", "regulation_proposals"],
                    "cross_domain": {
                        "regulation": "ai_regulation_is_emerging",
                        "information": "ai_reshaping_information_production",
                        "capital_allocation": "ai_investment_is_largest_capex_theme",
                    },
                    "key_personnel": [
                        {"name": "Sam Altman", "title": "OpenAI CEO", "influence": 0.65},
                        {"name": "Dario Amodei", "title": "Anthropic CEO", "influence": 0.50},
                        {"name": "Demis Hassabis", "title": "Google DeepMind CEO", "influence": 0.55},
                    ],
                    "confidence": "derived",
                },
            },
            "tier_3": {
                "startups": {
                    "name": "Tech Startups",
                    "entity": "VC-backed startups (aggregate)",
                    "influence": 0.30,
                    "controls": ["disruption_vectors", "talent_competition", "innovation_pace"],
                    "reports_to": ["vc_investors"],
                    "influenced_by": ["funding_availability", "interest_rates", "ipo_window"],
                    "cross_domain": {
                        "capital_allocation": "startup_funding_is_capital_allocation_frontier",
                    },
                    "confidence": "estimated",
                },
                "open_source": {
                    "name": "Open Source Community",
                    "entity": "Linux Foundation, Apache, CNCF, Hugging Face, etc.",
                    "influence": 0.35,
                    "controls": [
                        "infrastructure_standards", "ai_model_democratization",
                        "developer_tools", "protocol_standards",
                    ],
                    "reports_to": ["community"],
                    "influenced_by": ["corporate_sponsorship", "developer_adoption"],
                    "cross_domain": {
                        "regulation": "open_source_vs_proprietary_ai_debate",
                        "information": "open_source_ai_democratizes_information",
                    },
                    "confidence": "estimated",
                },
            },
        },
        "transmission": (
            "research_breakthrough -> corporate_adoption -> capex_cycle -> "
            "semiconductor_demand -> supply_chain_build -> productivity_gains -> "
            "economic_growth"
        ),
    },

    # ──────────────────────────────────────────────────────────────────────
    # 7. ENERGY — Who Controls Energy
    # ──────────────────────────────────────────────────────────────────────
    "energy": {
        "label": "Who Controls Energy",
        "actors": {
            "tier_1": {
                "opec_plus": {
                    "name": "OPEC+",
                    "entity": "Organization of the Petroleum Exporting Countries + Russia",
                    "influence": 0.90,
                    "controls": [
                        "oil_production_quotas", "spare_capacity", "oil_price_floor",
                        "strategic_supply_withholding",
                    ],
                    "reports_to": ["member_state_governments"],
                    "influenced_by": [
                        "budget_breakevens", "us_shale_competition",
                        "global_demand", "geopolitics",
                    ],
                    "cross_domain": {
                        "trade": "opec_decisions_affect_trade_balances",
                        "monetary_policy": "oil_prices_drive_inflation",
                        "fiscal_policy": "petro_state_budgets_depend_on_oil",
                    },
                    "key_personnel": [
                        {"name": "Prince Abdulaziz bin Salman", "title": "Saudi Energy Minister", "influence": 0.85},
                        {"name": "Haitham Al Ghais", "title": "OPEC Secretary General", "influence": 0.50},
                        {"name": "Alexander Novak", "title": "Russian Deputy PM (Energy)", "influence": 0.65},
                    ],
                    "confidence": "hard_data",
                },
                "russia": {
                    "name": "Russia (Energy)",
                    "entity": "Russian Federation — Energy Sector",
                    "influence": 0.65,
                    "controls": [
                        "natural_gas_to_europe", "arctic_oil", "pipeline_politics",
                        "opec_plus_compliance",
                    ],
                    "reports_to": ["kremlin"],
                    "influenced_by": ["sanctions", "war_costs", "budget_needs"],
                    "cross_domain": {
                        "trade": "russia_sanctions_reshape_energy_trade",
                        "fiscal_policy": "russia_budget_depends_on_oil_gas_revenue",
                    },
                    "key_personnel": [
                        {"name": "Vladimir Putin", "title": "President", "influence": 0.70},
                        {"name": "Gazprom leadership", "title": "State gas company", "influence": 0.50},
                        {"name": "Rosneft / Igor Sechin", "title": "State oil CEO", "influence": 0.55},
                    ],
                    "confidence": "derived",
                },
                "us_shale": {
                    "name": "US Shale Producers",
                    "entity": "ExxonMobil, Chevron, Pioneer, EOG, ConocoPhillips, etc.",
                    "influence": 0.70,
                    "controls": [
                        "us_oil_production", "rig_count", "drilled_but_uncompleted",
                        "capital_discipline", "lng_exports",
                    ],
                    "reports_to": ["shareholders"],
                    "influenced_by": ["oil_price", "interest_rates", "esg_pressure", "regulation"],
                    "cross_domain": {
                        "capital_allocation": "shale_capex_discipline_vs_growth",
                        "trade": "us_lng_exports_reshape_global_gas",
                        "regulation": "epa_methane_rules",
                    },
                    "key_personnel": [
                        {"name": "Darren Woods", "title": "ExxonMobil CEO", "influence": 0.60},
                        {"name": "Mike Wirth", "title": "Chevron CEO", "influence": 0.55},
                    ],
                    "confidence": "hard_data",
                },
            },
            "tier_2": {
                "refiners": {
                    "name": "Refiners",
                    "entity": "Valero, Marathon, Phillips 66, etc.",
                    "influence": 0.45,
                    "controls": ["refining_capacity", "crack_spreads", "product_supply"],
                    "reports_to": ["shareholders"],
                    "influenced_by": ["crude_prices", "product_demand", "maintenance_schedules"],
                    "cross_domain": {
                        "trade": "refining_capacity_affects_product_exports",
                    },
                    "confidence": "hard_data",
                },
                "pipelines": {
                    "name": "Pipeline Operators",
                    "entity": "Enterprise Products, Kinder Morgan, Williams, Energy Transfer",
                    "influence": 0.40,
                    "controls": ["transport_capacity", "takeaway_constraints", "storage"],
                    "reports_to": ["shareholders", "ferc"],
                    "influenced_by": ["permitting", "environmental_regulation", "volume_growth"],
                    "cross_domain": {
                        "regulation": "pipeline_permitting_is_political",
                    },
                    "confidence": "hard_data",
                },
                "utilities": {
                    "name": "Electric Utilities",
                    "entity": "NextEra, Duke, Southern, Dominion, etc.",
                    "influence": 0.40,
                    "controls": ["power_generation_mix", "grid_reliability", "rate_base_capex"],
                    "reports_to": ["state_pucs", "shareholders"],
                    "influenced_by": ["natural_gas_prices", "renewable_costs", "load_growth"],
                    "cross_domain": {
                        "technology": "ai_data_center_power_demand",
                        "regulation": "utility_rate_cases",
                    },
                    "confidence": "hard_data",
                },
            },
            "tier_3": {
                "renewables": {
                    "name": "Renewable Energy",
                    "entity": "Solar, Wind, Battery Storage Companies",
                    "influence": 0.35,
                    "controls": ["clean_energy_capacity", "cost_curves", "grid_penetration"],
                    "reports_to": ["shareholders", "regulators"],
                    "influenced_by": ["subsidies", "interest_rates", "permitting", "grid_interconnection"],
                    "cross_domain": {
                        "fiscal_policy": "ira_subsidies_drive_renewable_investment",
                        "technology": "battery_tech_determines_grid_storage",
                    },
                    "confidence": "hard_data",
                },
                "nuclear": {
                    "name": "Nuclear Power",
                    "entity": "Cameco, NuScale, existing fleet operators",
                    "influence": 0.25,
                    "controls": ["baseload_power", "uranium_demand", "smr_development"],
                    "reports_to": ["nrc", "shareholders"],
                    "influenced_by": ["public_sentiment", "licensing_timelines", "ai_power_demand"],
                    "cross_domain": {
                        "technology": "ai_power_demand_reviving_nuclear",
                        "regulation": "nrc_licensing_is_bottleneck",
                    },
                    "confidence": "derived",
                },
                "grid_operators": {
                    "name": "Grid Operators / ISOs",
                    "entity": "PJM, ERCOT, CAISO, MISO, etc.",
                    "influence": 0.35,
                    "controls": ["dispatch_order", "capacity_markets", "interconnection_queues"],
                    "reports_to": ["ferc", "state_regulators"],
                    "influenced_by": ["load_growth", "extreme_weather", "generation_mix"],
                    "cross_domain": {
                        "technology": "grid_operators_gate_data_center_buildout",
                    },
                    "confidence": "hard_data",
                },
            },
        },
        "transmission": (
            "opec_decision -> crude_price -> refining_margins -> gasoline_price -> "
            "cpi_energy -> fed_inflation_expectations -> rate_decision -> "
            "all_asset_classes"
        ),
    },

    # ──────────────────────────────────────────────────────────────────────
    # 8. TRADE — Who Controls Global Trade
    # ──────────────────────────────────────────────────────────────────────
    "trade": {
        "label": "Who Controls Global Trade",
        "actors": {
            "tier_1": {
                "us_trade_rep": {
                    "name": "US Trade Representative",
                    "entity": "USTR",
                    "influence": 0.85,
                    "controls": [
                        "tariff_policy", "trade_agreements", "section_301",
                        "wto_disputes", "trade_enforcement",
                    ],
                    "reports_to": ["white_house"],
                    "influenced_by": ["industry_lobbying", "geopolitics", "domestic_politics"],
                    "cross_domain": {
                        "fiscal_policy": "tariffs_are_revenue",
                        "technology": "trade_rep_controls_tech_export_policy",
                    },
                    "key_personnel": [
                        {"name": "USTR", "title": "US Trade Representative", "influence": 0.80},
                    ],
                    "confidence": "hard_data",
                },
                "china_mofcom": {
                    "name": "China Ministry of Commerce",
                    "entity": "MOFCOM",
                    "influence": 0.80,
                    "controls": [
                        "china_tariffs", "rare_earth_export_controls",
                        "foreign_investment_approval", "anti_dumping",
                    ],
                    "reports_to": ["state_council", "ccp"],
                    "influenced_by": ["ccp_politburo", "export_sector", "geopolitics"],
                    "cross_domain": {
                        "technology": "china_controls_rare_earth_supply",
                        "energy": "china_is_largest_energy_importer",
                    },
                    "confidence": "derived",
                },
                "eu_trade": {
                    "name": "EU Trade Commissioner",
                    "entity": "European Commission — DG Trade",
                    "influence": 0.70,
                    "controls": [
                        "eu_tariffs", "trade_agreements", "carbon_border_adjustment",
                        "digital_services_regulation",
                    ],
                    "reports_to": ["eu_commission", "eu_parliament"],
                    "influenced_by": ["member_state_interests", "industry_lobbying"],
                    "cross_domain": {
                        "regulation": "eu_regulation_has_global_extraterritorial_effect",
                        "technology": "eu_digital_regulation_shapes_tech",
                        "energy": "eu_carbon_border_tax",
                    },
                    "confidence": "hard_data",
                },
            },
            "tier_2": {
                "wto": {
                    "name": "World Trade Organization",
                    "entity": "WTO",
                    "influence": 0.40,
                    "controls": ["trade_dispute_resolution", "trade_rules", "most_favored_nation"],
                    "reports_to": ["member_states"],
                    "influenced_by": ["us_china_rivalry", "appellate_body_crisis"],
                    "cross_domain": {
                        "regulation": "wto_rules_constrain_domestic_regulation",
                    },
                    "confidence": "hard_data",
                },
                "shipping_companies": {
                    "name": "Global Shipping",
                    "entity": "Maersk, MSC, CMA CGM, COSCO, etc.",
                    "influence": 0.50,
                    "controls": [
                        "container_rates", "route_scheduling", "capacity_allocation",
                        "port_congestion",
                    ],
                    "reports_to": ["shareholders"],
                    "influenced_by": ["trade_volumes", "fuel_costs", "geopolitical_disruptions"],
                    "cross_domain": {
                        "energy": "shipping_fuel_costs_affect_trade_costs",
                    },
                    "key_personnel": [
                        {"name": "Maersk", "title": "Largest container line", "influence": 0.45},
                        {"name": "MSC", "title": "#2 container line", "influence": 0.40},
                    ],
                    "confidence": "hard_data",
                },
                "port_operators": {
                    "name": "Major Port Operators",
                    "entity": "DP World, PSA, Hutchison, APM Terminals",
                    "influence": 0.35,
                    "controls": ["port_throughput", "terminal_capacity", "logistics_bottlenecks"],
                    "reports_to": ["shareholders", "port_authorities"],
                    "influenced_by": ["trade_volumes", "automation", "labor_relations"],
                    "cross_domain": {
                        "technology": "port_automation_investment",
                    },
                    "confidence": "derived",
                },
            },
            "tier_3": {
                "customs": {
                    "name": "Customs Agencies",
                    "entity": "CBP (US), HMRC (UK), etc.",
                    "influence": 0.30,
                    "controls": ["border_enforcement", "tariff_collection", "import_inspections"],
                    "reports_to": ["government"],
                    "influenced_by": ["trade_policy", "security_concerns", "staffing"],
                    "cross_domain": {
                        "fiscal_policy": "customs_collects_tariff_revenue",
                    },
                    "confidence": "hard_data",
                },
                "freight_forwarders": {
                    "name": "Freight Forwarders & Logistics",
                    "entity": "Kuehne+Nagel, DHL, DB Schenker, C.H. Robinson",
                    "influence": 0.25,
                    "controls": ["logistics_optimization", "multimodal_transport", "customs_brokerage"],
                    "reports_to": ["shippers"],
                    "influenced_by": ["capacity_availability", "fuel_surcharges", "digital_platforms"],
                    "cross_domain": {
                        "technology": "logistics_tech_disruption",
                    },
                    "confidence": "derived",
                },
                "supply_chain_finance": {
                    "name": "Supply Chain Finance",
                    "entity": "Trade finance banks, factoring companies",
                    "influence": 0.30,
                    "controls": ["trade_credit", "letters_of_credit", "receivables_factoring"],
                    "reports_to": ["banks", "regulators"],
                    "influenced_by": ["interest_rates", "credit_risk", "trade_volumes"],
                    "cross_domain": {
                        "monetary_policy": "trade_finance_costs_track_rates",
                        "capital_allocation": "trade_finance_is_credit_allocation",
                    },
                    "confidence": "derived",
                },
            },
        },
        "transmission": (
            "tariff_announcement -> supply_chain_rerouting -> cost_increase -> "
            "corporate_margins -> earnings_revision -> stock_price -> "
            "consumer_price -> inflation"
        ),
    },
}


# ══════════════════════════════════════════════════════════════════════════
# LEVER CHAIN TEMPLATES — event → sequence of effects
# ══════════════════════════════════════════════════════════════════════════

LEVER_CHAINS: dict[str, list[dict[str, str]]] = {
    "interest_rate_hike": [
        {"actor": "fed", "domain": "monetary_policy", "action": "raises federal funds rate"},
        {"actor": "banks", "domain": "monetary_policy", "action": "raise lending rates"},
        {"actor": "banks", "domain": "monetary_policy", "action": "tighten lending standards"},
        {"actor": "mortgage_market", "domain": "capital_allocation", "action": "mortgage rates rise"},
        {"actor": "housing", "domain": "capital_allocation", "action": "home sales decline"},
        {"actor": "construction", "domain": "fiscal_policy", "action": "building permits drop"},
        {"actor": "employment", "domain": "fiscal_policy", "action": "construction jobs decline"},
        {"actor": "consumer", "domain": "fiscal_policy", "action": "spending decreases"},
        {"actor": "fed", "domain": "monetary_policy", "action": "monitors for target inflation"},
    ],
    "interest_rate_cut": [
        {"actor": "fed", "domain": "monetary_policy", "action": "lowers federal funds rate"},
        {"actor": "banks", "domain": "monetary_policy", "action": "lower lending rates"},
        {"actor": "bond_market", "domain": "monetary_policy", "action": "yields decline, prices rise"},
        {"actor": "hedge_funds", "domain": "capital_allocation", "action": "increase risk appetite"},
        {"actor": "retail", "domain": "capital_allocation", "action": "pile into equities"},
        {"actor": "private_equity", "domain": "capital_allocation", "action": "leverage up for buyouts"},
        {"actor": "economy", "domain": "fiscal_policy", "action": "credit expansion stimulates growth"},
    ],
    "tariff_war": [
        {"actor": "us_trade_rep", "domain": "trade", "action": "imposes new tariffs"},
        {"actor": "china_mofcom", "domain": "trade", "action": "retaliates with counter-tariffs"},
        {"actor": "shipping_companies", "domain": "trade", "action": "reroute supply chains"},
        {"actor": "supply_chain_finance", "domain": "trade", "action": "trade credit costs rise"},
        {"actor": "corporations", "domain": "capital_allocation", "action": "margins compressed"},
        {"actor": "analyst_research", "domain": "information", "action": "downgrade earnings estimates"},
        {"actor": "fed", "domain": "monetary_policy", "action": "assesses inflationary impact"},
    ],
    "oil_supply_cut": [
        {"actor": "opec_plus", "domain": "energy", "action": "cuts production quotas"},
        {"actor": "us_shale", "domain": "energy", "action": "evaluates marginal well economics"},
        {"actor": "refiners", "domain": "energy", "action": "input costs rise, margins adjust"},
        {"actor": "consumer", "domain": "fiscal_policy", "action": "gasoline prices spike"},
        {"actor": "fed_communications", "domain": "information", "action": "inflation expectations rise"},
        {"actor": "fed", "domain": "monetary_policy", "action": "hawkish tilt on energy inflation"},
        {"actor": "bond_market", "domain": "monetary_policy", "action": "yields rise on inflation fear"},
    ],
    "tech_antitrust": [
        {"actor": "doj", "domain": "regulation", "action": "files antitrust suit"},
        {"actor": "wsj", "domain": "information", "action": "front-page coverage"},
        {"actor": "cloud_providers", "domain": "technology", "action": "stock prices drop"},
        {"actor": "hedge_funds", "domain": "capital_allocation", "action": "reposition tech exposure"},
        {"actor": "congressional_committees", "domain": "regulation", "action": "hold hearings"},
        {"actor": "industry_lobbyists", "domain": "regulation", "action": "mobilize defense"},
        {"actor": "venture_capital", "domain": "capital_allocation", "action": "reassess startup valuations"},
    ],
    "bank_stress": [
        {"actor": "fed_supervision", "domain": "regulation", "action": "identifies capital shortfall"},
        {"actor": "banks", "domain": "monetary_policy", "action": "tighten lending dramatically"},
        {"actor": "bond_market", "domain": "monetary_policy", "action": "credit spreads blow out"},
        {"actor": "private_equity", "domain": "capital_allocation", "action": "distressed opportunities emerge"},
        {"actor": "fed", "domain": "monetary_policy", "action": "opens emergency lending facilities"},
        {"actor": "treasury_fiscal", "domain": "fiscal_policy", "action": "considers backstop measures"},
        {"actor": "cnbc", "domain": "information", "action": "bank run narrative amplified"},
        {"actor": "retail", "domain": "capital_allocation", "action": "panic selling"},
    ],
    "ai_capex_boom": [
        {"actor": "ai_companies", "domain": "technology", "action": "announce massive capex plans"},
        {"actor": "chip_makers", "domain": "technology", "action": "demand surge, backlog extends"},
        {"actor": "cloud_providers", "domain": "technology", "action": "build new data centers"},
        {"actor": "utilities", "domain": "energy", "action": "power demand spikes"},
        {"actor": "nuclear", "domain": "energy", "action": "revival narrative strengthens"},
        {"actor": "sovereign_wealth", "domain": "capital_allocation", "action": "increase tech allocation"},
        {"actor": "analyst_research", "domain": "information", "action": "upgrade semi sector"},
    ],
    "quantitative_tightening": [
        {"actor": "fed", "domain": "monetary_policy", "action": "reduces balance sheet (QT)"},
        {"actor": "treasury", "domain": "monetary_policy", "action": "must issue more to private market"},
        {"actor": "bond_market", "domain": "monetary_policy", "action": "supply overwhelms demand, yields rise"},
        {"actor": "banks", "domain": "monetary_policy", "action": "reserves drain, tighten lending"},
        {"actor": "market_makers", "domain": "monetary_policy", "action": "liquidity deteriorates"},
        {"actor": "hedge_funds", "domain": "capital_allocation", "action": "deleverage on tighter conditions"},
        {"actor": "retail", "domain": "capital_allocation", "action": "money market yields attract cash"},
    ],
}


# ══════════════════════════════════════════════════════════════════════════
# CROSS-DOMAIN ACTOR INDEX (built at import time)
# ══════════════════════════════════════════════════════════════════════════

def _build_actor_index() -> dict[str, list[dict[str, str]]]:
    """Build an index of actor_id -> list of {domain, tier} appearances."""
    index: dict[str, list[dict[str, str]]] = defaultdict(list)
    for domain_key, domain in LEVER_HIERARCHY.items():
        actors = domain.get("actors", {})
        for tier_key, tier_actors in actors.items():
            for actor_id, actor_data in tier_actors.items():
                index[actor_id].append({
                    "domain": domain_key,
                    "tier": tier_key,
                    "name": actor_data.get("name", actor_id),
                    "influence": actor_data.get("influence", 0.0),
                })
    return dict(index)


_ACTOR_INDEX: dict[str, list[dict[str, str]]] = _build_actor_index()


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API — the 5 required methods
# ══════════════════════════════════════════════════════════════════════════

def get_lever_hierarchy() -> dict[str, Any]:
    """Return the full 8-domain lever hierarchy.

    Returns the complete LEVER_HIERARCHY dict plus summary metadata.
    """
    domain_summaries = {}
    for domain_key, domain in LEVER_HIERARCHY.items():
        actor_count = 0
        tier_counts: dict[str, int] = {}
        for tier_key, tier_actors in domain.get("actors", {}).items():
            tier_counts[tier_key] = len(tier_actors)
            actor_count += len(tier_actors)
        domain_summaries[domain_key] = {
            "label": domain["label"],
            "actor_count": actor_count,
            "tiers": tier_counts,
            "transmission": domain.get("transmission", ""),
        }

    return {
        "hierarchy": LEVER_HIERARCHY,
        "summary": domain_summaries,
        "total_domains": len(LEVER_HIERARCHY),
        "total_actors": sum(s["actor_count"] for s in domain_summaries.values()),
    }


def get_lever_domain(domain: str) -> dict[str, Any]:
    """Return a single lever domain with full actor details.

    Args:
        domain: One of the 8 domain keys (e.g. 'monetary_policy', 'trade').

    Returns:
        The domain dict or an error dict if not found.
    """
    if domain not in LEVER_HIERARCHY:
        available = list(LEVER_HIERARCHY.keys())
        return {"error": f"Unknown domain '{domain}'", "available_domains": available}

    data = LEVER_HIERARCHY[domain]

    # Enrich with cross-domain references for each actor
    enriched_actors: dict[str, dict] = {}
    for tier_key, tier_actors in data.get("actors", {}).items():
        enriched_tier: dict[str, dict] = {}
        for actor_id, actor_data in tier_actors.items():
            enriched = dict(actor_data)
            # Add appearances in other domains
            other_appearances = [
                a for a in _ACTOR_INDEX.get(actor_id, [])
                if a["domain"] != domain
            ]
            if other_appearances:
                enriched["also_appears_in"] = other_appearances
            enriched_tier[actor_id] = enriched
        enriched_actors[tier_key] = enriched_tier

    return {
        "domain": domain,
        "label": data["label"],
        "actors": enriched_actors,
        "transmission": data.get("transmission", ""),
    }


def trace_lever_chain(event: str) -> list[dict[str, str]]:
    """Trace the chain of effects from a named event.

    Args:
        event: A known event key (e.g. 'interest_rate_hike', 'tariff_war')
               or a free-text event description that we fuzzy-match.

    Returns:
        Ordered list of dicts: {actor, domain, action} showing the causal chain.
    """
    # Exact match first
    if event in LEVER_CHAINS:
        return LEVER_CHAINS[event]

    # Fuzzy match: find chains whose key contains any word from the query
    event_lower = event.lower().replace(" ", "_")
    query_words = set(event_lower.replace("_", " ").split())

    best_match: str | None = None
    best_score = 0
    for chain_key in LEVER_CHAINS:
        key_words = set(chain_key.replace("_", " ").split())
        overlap = len(query_words & key_words)
        if overlap > best_score:
            best_score = overlap
            best_match = chain_key

    if best_match and best_score > 0:
        chain = LEVER_CHAINS[best_match]
        return [{"_matched_event": best_match}] + chain

    # No match — build a generic chain from the hierarchy
    return [
        {
            "actor": "unknown",
            "domain": "unknown",
            "action": f"No pre-built chain for '{event}'",
        },
        {
            "note": "Available events",
            "events": list(LEVER_CHAINS.keys()),  # type: ignore[dict-item]
        },
    ]


def find_cross_domain_actors(engine: Engine | None = None) -> list[dict[str, Any]]:
    """Find actors who appear in 2+ lever domains — the most powerful players.

    These are the actors who sit at the intersection of multiple power
    structures.  They can pull levers in multiple domains simultaneously.

    Args:
        engine: Optional DB engine for enrichment with live data.

    Returns:
        List of cross-domain actors sorted by total influence, each with:
        - actor_id, name, domains, total_influence, max_tier, cross_domain_links
    """
    cross_domain: list[dict[str, Any]] = []

    for actor_id, appearances in _ACTOR_INDEX.items():
        if len(appearances) < 2:
            continue

        domains = [a["domain"] for a in appearances]
        total_influence = sum(a.get("influence", 0) for a in appearances)
        max_influence = max(a.get("influence", 0) for a in appearances)
        best_tier = min(a["tier"] for a in appearances)  # tier_1 < tier_2
        name = appearances[0].get("name", actor_id)

        # Gather cross-domain links from the hierarchy
        cross_links: list[dict[str, str]] = []
        for app in appearances:
            domain_data = LEVER_HIERARCHY.get(app["domain"], {})
            actors = domain_data.get("actors", {})
            tier_actors = actors.get(app["tier"], {})
            actor_data = tier_actors.get(actor_id, {})
            for target_domain, description in actor_data.get("cross_domain", {}).items():
                cross_links.append({
                    "from_domain": app["domain"],
                    "to_domain": target_domain,
                    "description": description,
                })

        cross_domain.append({
            "actor_id": actor_id,
            "name": name,
            "domains": domains,
            "domain_count": len(domains),
            "total_influence": round(total_influence, 2),
            "max_influence": round(max_influence, 2),
            "best_tier": best_tier,
            "appearances": appearances,
            "cross_domain_links": cross_links,
        })

    # Sort by max influence descending, then by domain count
    cross_domain.sort(key=lambda x: (-x["max_influence"], -x["domain_count"]))

    # If we have a DB engine, try to enrich with recent activity
    if engine is not None:
        try:
            _enrich_cross_domain_with_signals(engine, cross_domain)
        except Exception as exc:
            log.debug("Could not enrich cross-domain actors with signals: {e}", e=str(exc))

    return cross_domain


def _enrich_cross_domain_with_signals(
    engine: Engine, actors: list[dict[str, Any]]
) -> None:
    """Enrich cross-domain actor list with recent signal data from DB."""
    try:
        from intelligence.lever_pullers import get_active_lever_events
        events = get_active_lever_events(engine, days=30)
        if not events:
            return

        # Build lookup of recent events by actor name (fuzzy)
        event_by_name: dict[str, list] = defaultdict(list)
        for evt in events:
            name = evt.get("actor", "") if isinstance(evt, dict) else getattr(evt, "actor", "")
            name_lower = name.lower() if name else ""
            event_by_name[name_lower].append(evt)

        for actor in actors:
            actor_name_lower = actor["name"].lower()
            matched_events = []
            for evt_name, evts in event_by_name.items():
                if (actor_name_lower in evt_name or evt_name in actor_name_lower
                        or actor["actor_id"] in evt_name):
                    matched_events.extend(evts)
            if matched_events:
                actor["recent_signals"] = matched_events[:5]
    except Exception as exc:
        log.debug("Signal enrichment skipped: {e}", e=str(exc))


def generate_lever_report(engine: Engine | None = None) -> str:
    """Generate a narrative report: who's pulling what lever right now and why.

    Combines the static hierarchy with live signal data (if engine provided)
    to produce a human-readable briefing.

    Args:
        engine: Optional DB engine for live signal data.

    Returns:
        Multi-paragraph string report.
    """
    lines: list[str] = []
    lines.append("=" * 72)
    lines.append("GLOBAL LEVER MAP — Who's Pulling What Right Now")
    lines.append("=" * 72)
    lines.append("")

    # Section 1: Domain overview
    lines.append("LEVER DOMAINS")
    lines.append("-" * 40)
    for domain_key, domain in LEVER_HIERARCHY.items():
        actors = domain.get("actors", {})
        tier_1 = actors.get("tier_1", {})
        tier_1_names = [a.get("name", k) for k, a in tier_1.items()]
        lines.append(f"  {domain['label']}")
        lines.append(f"    Top actors: {', '.join(tier_1_names[:4])}")
        if domain.get("transmission"):
            lines.append(f"    Transmission: {domain['transmission'][:80]}...")
        lines.append("")

    # Section 2: Cross-domain power brokers
    lines.append("CROSS-DOMAIN POWER BROKERS")
    lines.append("-" * 40)
    cross = find_cross_domain_actors(engine)
    for actor in cross[:10]:
        domains_str = ", ".join(actor["domains"])
        lines.append(
            f"  {actor['name']} — {actor['domain_count']} domains "
            f"(influence: {actor['max_influence']:.2f}) [{domains_str}]"
        )
    lines.append("")

    # Section 3: Active lever chains
    lines.append("PRE-BUILT EVENT CHAINS")
    lines.append("-" * 40)
    for chain_key in LEVER_CHAINS:
        chain = LEVER_CHAINS[chain_key]
        steps = " -> ".join(step.get("actor", "?") for step in chain[:5])
        lines.append(f"  {chain_key}: {steps}...")
    lines.append("")

    # Section 4: Live signals (if engine available)
    if engine is not None:
        lines.append("LIVE SIGNAL ACTIVITY")
        lines.append("-" * 40)
        try:
            from intelligence.lever_pullers import get_active_lever_events
            events = get_active_lever_events(engine, days=14)
            if events:
                for evt in events[:15]:
                    if isinstance(evt, dict):
                        actor = evt.get("actor", "?")
                        action = evt.get("action", evt.get("description", "?"))
                        lines.append(f"  [{actor}] {action}")
                    else:
                        lines.append(f"  {evt}")
            else:
                lines.append("  No recent lever events detected.")
        except Exception as exc:
            lines.append(f"  Could not fetch live signals: {exc}")
    else:
        lines.append("LIVE SIGNALS: No database connection — static report only.")

    lines.append("")
    lines.append("=" * 72)
    return "\n".join(lines)
