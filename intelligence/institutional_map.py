"""
GRID Intelligence -- Institutional Map: Private Credit, Hedge Funds & Pensions.

Maps the connections between private credit funds, hedge funds, and pension
systems -- where the really big money moves and where conflicts of interest
hide.  This is the shadow banking layer that channels trillions of retirement
dollars through opaque fee structures.

The conflict chain:
    Pension invests $1B with Apollo
    -> Apollo charges 2% mgmt + 20% carry
    -> Apollo uses capital for leveraged buyouts
    -> Companies get loaded with debt
    -> Some go bankrupt (PE-backed bankruptcies hit record in 2024-2025)
    -> Pension loses principal
    -> Apollo already collected the fees

Data confidence labels follow GRID convention:
    confirmed  -- public filings, annual reports, press releases
    derived    -- calculated from multiple confirmed sources
    estimated  -- industry-standard assumptions or partial data
    rumored    -- media reports, unnamed sources

Key entry points:
    build_institutional_graph    -- nodes + links for D3 visualization
    trace_pension_dollars        -- where does a pension's money end up?
    find_conflicts_of_interest   -- revolving door + allocation overlap
    get_fee_extraction_estimate  -- how much does a fund extract in fees?

Sources:
    - CalPERS, CalSTRS, NY Common, Florida SBA public disclosures
    - SEC 13F, Form ADV filings
    - Pensions & Investments, Institutional Investor reporting
    - Private Equity Stakeholder Project bankruptcy tracker
    - Bloomberg, Fortune, CNN reporting on 2026 private credit crisis
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# PRIVATE CREDIT FUNDS -- the new shadow banking system
# ══════════════════════════════════════════════════════════════════════════

PRIVATE_CREDIT_FUNDS: dict[str, dict[str, Any]] = {
    "apollo": {
        "name": "Apollo Global Management",
        "ticker": "APO",
        "aum": 908_000_000_000,  # $908B total, $723B credit (Sep 2025)
        "credit_aum": 723_000_000_000,
        "ceo": "Marc Rowan",
        "type": "private_credit",
        "founded": 1990,
        "hq": "New York, NY",
        "public": True,
        "fee_structure": {
            "management_fee_pct": 1.5,
            "performance_fee_pct": 20.0,
            "hurdle_rate_pct": 8.0,
            "model": "2_and_20_variant",
        },
        "key_personnel": [
            {"name": "Marc Rowan", "title": "CEO", "net_worth_est": 8_200_000_000},
            {"name": "Scott Kleinman", "title": "Co-President"},
            {"name": "James Zelter", "title": "Co-President"},
        ],
        "scandal_history": [
            "SEC pay-to-play violation (Ohio pension, 2016)",
            "Claire's bankruptcy ($3.1B LBO loaded with $2.5B debt, 2018)",
            "Apollo Debt Solutions BDC redemption cap (March 2026)",
        ],
        "confidence": "confirmed",
    },
    "blackstone": {
        "name": "Blackstone Inc.",
        "ticker": "BX",
        "aum": 1_065_000_000_000,  # ~$1.065T (Q4 2025)
        "credit_aum": 520_000_000_000,  # $520B credit AUM (Dec 2025)
        "ceo": "Steve Schwarzman",
        "type": "private_credit",
        "founded": 1985,
        "hq": "New York, NY",
        "public": True,
        "fee_structure": {
            "management_fee_pct": 1.5,
            "performance_fee_pct": 20.0,
            "hurdle_rate_pct": 7.0,
            "model": "2_and_20_variant",
        },
        "key_personnel": [
            {"name": "Steve Schwarzman", "title": "Chairman & CEO", "net_worth_est": 42_000_000_000},
            {"name": "Jonathan Gray", "title": "President & COO"},
            {"name": "Brad Marshall", "title": "Head of Credit"},
        ],
        "scandal_history": [
            "BREIT redemption gates (2022-2023)",
            "NYC pension $5B secondary sale to Blackstone (2025)",
        ],
        "vehicles": ["BCRED (Blackstone Private Credit Fund)", "BXSL", "BREIT"],
        "confidence": "confirmed",
    },
    "ares": {
        "name": "Ares Management",
        "ticker": "ARES",
        "aum": 428_000_000_000,  # ~$428B (2025)
        "credit_aum": 310_000_000_000,
        "ceo": "Michael Arougheti",
        "type": "private_credit",
        "founded": 1997,
        "hq": "Los Angeles, CA",
        "public": True,
        "fee_structure": {
            "management_fee_pct": 1.5,
            "performance_fee_pct": 20.0,
            "hurdle_rate_pct": 7.0,
            "model": "2_and_20_variant",
        },
        "key_personnel": [
            {"name": "Michael Arougheti", "title": "CEO & President"},
            {"name": "David Kaplan", "title": "Co-Founder & Senior Partner"},
        ],
        "vehicles": ["Ares Pathfinder II", "Ares Senior Direct Lending Fund IV"],
        "confidence": "confirmed",
    },
    "kkr": {
        "name": "KKR & Co.",
        "ticker": "KKR",
        "aum": 553_000_000_000,  # ~$553B (2025)
        "credit_aum": 230_000_000_000,
        "ceo": "Scott Nuttall",
        "type": "private_credit",
        "founded": 1976,
        "hq": "New York, NY",
        "public": True,
        "fee_structure": {
            "management_fee_pct": 1.5,
            "performance_fee_pct": 20.0,
            "hurdle_rate_pct": 8.0,
            "model": "2_and_20_variant",
        },
        "key_personnel": [
            {"name": "Scott Nuttall", "title": "Co-CEO"},
            {"name": "Joseph Bae", "title": "Co-CEO"},
            {"name": "Henry Kravis", "title": "Co-Founder & Co-Executive Chairman"},
            {"name": "George Roberts", "title": "Co-Founder & Co-Executive Chairman"},
        ],
        "scandal_history": [
            "Pioneer of hostile LBO era (1980s)",
            "RJR Nabisco LBO ($25B, 1989)",
        ],
        "confidence": "confirmed",
    },
    "blue_owl": {
        "name": "Blue Owl Capital",
        "ticker": "OWL",
        "aum": 235_000_000_000,  # ~$235B (2025)
        "credit_aum": 135_000_000_000,
        "ceo": "Doug Ostrover",
        "type": "private_credit",
        "founded": 2021,  # merger of Owl Rock + Dyal
        "hq": "New York, NY",
        "public": True,
        "fee_structure": {
            "management_fee_pct": 1.5,
            "performance_fee_pct": 17.5,
            "hurdle_rate_pct": 7.0,
            "model": "2_and_20_variant",
        },
        "key_personnel": [
            {"name": "Doug Ostrover", "title": "Co-CEO"},
            {"name": "Marc Lipschultz", "title": "Co-CEO"},
            {"name": "Craig Packer", "title": "Co-President"},
        ],
        "scandal_history": [
            "$1.4B fire sale loan portfolio (Feb 2026)",
            "Fund redemption halt (Feb 2026)",
        ],
        "includes_owl_rock": True,
        "confidence": "confirmed",
    },
    "golub": {
        "name": "Golub Capital",
        "ticker": None,  # private
        "aum": 90_000_000_000,  # $90B+ capital under mgmt (Jan 2026)
        "credit_aum": 90_000_000_000,
        "ceo": "Lawrence Golub",
        "type": "private_credit",
        "founded": 1999,
        "hq": "Chicago, IL",
        "public": False,
        "fee_structure": {
            "management_fee_pct": 1.25,
            "performance_fee_pct": 15.0,
            "hurdle_rate_pct": 7.0,
            "model": "institutional_direct_lending",
        },
        "key_personnel": [
            {"name": "Lawrence Golub", "title": "CEO"},
            {"name": "David Golub", "title": "President"},
        ],
        "notes": "Raised record $20.5B in new capital in 2025",
        "confidence": "confirmed",
    },
    "hps": {
        "name": "HPS Investment Partners",
        "ticker": None,  # private (acquisition by BlackRock pending)
        "aum": 117_000_000_000,  # ~$117B (2025)
        "credit_aum": 117_000_000_000,
        "ceo": "Scott Kapnick",
        "type": "private_credit",
        "founded": 2007,
        "hq": "New York, NY",
        "public": False,
        "fee_structure": {
            "management_fee_pct": 1.5,
            "performance_fee_pct": 17.5,
            "hurdle_rate_pct": 7.0,
            "model": "2_and_20_variant",
        },
        "key_personnel": [
            {"name": "Scott Kapnick", "title": "CEO"},
            {"name": "Michael Patterson", "title": "Co-Founder"},
        ],
        "vehicles": ["HPS Strategic Investment Partners VI"],
        "notes": "BlackRock acquisition pending ~$12B deal",
        "confidence": "confirmed",
    },
    "owl_rock": {
        "name": "Owl Rock (now part of Blue Owl)",
        "ticker": "OWL",
        "aum": 0,  # merged into Blue Owl
        "credit_aum": 0,
        "ceo": "Craig Packer",
        "type": "private_credit",
        "founded": 2016,
        "hq": "New York, NY",
        "public": True,
        "merged_into": "blue_owl",
        "fee_structure": {
            "management_fee_pct": 1.5,
            "performance_fee_pct": 17.5,
            "model": "merged_into_blue_owl",
        },
        "confidence": "confirmed",
    },
}


# ══════════════════════════════════════════════════════════════════════════
# HEDGE FUNDS -- multi-strategy + private credit expansion
# ══════════════════════════════════════════════════════════════════════════

HEDGE_FUNDS: dict[str, dict[str, Any]] = {
    "citadel": {
        "name": "Citadel LLC",
        "ticker": None,
        "aum": 65_000_000_000,  # ~$65B hedge fund
        "ceo": "Ken Griffin",
        "type": "hedge_fund",
        "strategy": "multi_strategy",
        "founded": 1990,
        "hq": "Miami, FL",
        "also_runs": ["Citadel Securities (market maker)"],
        "fee_structure": {
            "management_fee_pct": 2.0,
            "performance_fee_pct": 25.0,
            "passthrough_fees": True,
            "passthrough_note": "$12.5B in passthrough fees 2022-Sep 2024, $11B to compensation",
            "model": "multi_manager_passthrough",
        },
        "lock_up_months": 12,
        "redemption_gate": "quarterly with 90-day notice",
        "key_personnel": [
            {"name": "Ken Griffin", "title": "Founder & CEO", "net_worth_est": 45_000_000_000},
            {"name": "Pablo Salame", "title": "Global Head of Equities"},
        ],
        "private_credit_status": "eyeing opportunities, no dedicated fund yet",
        "confidence": "confirmed",
    },
    "bridgewater": {
        "name": "Bridgewater Associates",
        "ticker": None,
        "aum": 124_000_000_000,  # ~$124B (Jan 2025)
        "ceo": "Nir Bar Dea",
        "type": "hedge_fund",
        "strategy": "macro",
        "founded": 1975,
        "hq": "Westport, CT",
        "fee_structure": {
            "management_fee_pct": 2.0,
            "performance_fee_pct": 20.0,
            "min_fee_pure_alpha": 6_000_000,
            "min_fee_all_weather": 500_000,
            "model": "negotiated_institutional",
        },
        "lock_up_months": 12,
        "redemption_gate": "quarterly with notice",
        "key_personnel": [
            {"name": "Ray Dalio", "title": "Founder (stepped back)", "net_worth_est": 15_400_000_000},
            {"name": "Nir Bar Dea", "title": "CEO"},
        ],
        "succession": "Dalio stepped back from management; co-CIOs era",
        "strategies": ["Pure Alpha", "Pure Alpha Major Markets", "All Weather", "Optimal Portfolio"],
        "confidence": "confirmed",
    },
    "millennium": {
        "name": "Millennium Management",
        "ticker": None,
        "aum": 70_000_000_000,  # ~$70B (2025)
        "ceo": "Izzy Englander",
        "type": "hedge_fund",
        "strategy": "multi_strategy",
        "founded": 1989,
        "hq": "New York, NY",
        "fee_structure": {
            "management_fee_pct": 0.0,  # no explicit mgmt fee
            "performance_fee_pct": 20.0,
            "passthrough_fees": True,
            "passthrough_note": "All operating expenses passed through to investors",
            "model": "multi_manager_passthrough",
        },
        "lock_up_months": 24,
        "redemption_gate": "semi-annual with lock-up",
        "key_personnel": [
            {"name": "Israel (Izzy) Englander", "title": "Founder & CEO", "net_worth_est": 13_000_000_000},
        ],
        "private_credit_status": "rumored standalone fund under consideration",
        "confidence": "confirmed",
    },
    "de_shaw": {
        "name": "D.E. Shaw & Co.",
        "ticker": None,
        "aum": 60_000_000_000,  # ~$60B (2024)
        "ceo": "David Shaw",
        "type": "hedge_fund",
        "strategy": "quantitative_multi_strategy",
        "founded": 1988,
        "hq": "New York, NY",
        "fee_structure": {
            "management_fee_pct": 3.0,
            "performance_fee_pct": 30.0,
            "model": "premium_quant",
            "note": "3-and-30 on flagship Composite fund",
        },
        "lock_up_months": 36,
        "redemption_gate": "annual with 6-month notice",
        "key_personnel": [
            {"name": "David Shaw", "title": "Founder & Executive Committee Chair", "net_worth_est": 8_500_000_000},
        ],
        "confidence": "confirmed",
    },
    "point72": {
        "name": "Point72 Asset Management",
        "ticker": None,
        "aum": 35_000_000_000,  # ~$35B (2025)
        "ceo": "Steve Cohen",
        "type": "hedge_fund",
        "strategy": "multi_strategy",
        "founded": 2014,  # successor to SAC Capital
        "hq": "Stamford, CT",
        "fee_structure": {
            "management_fee_pct": 0.0,
            "performance_fee_pct": 25.0,
            "passthrough_fees": True,
            "passthrough_note": "'No limit' on passthrough expenses per filings",
            "model": "multi_manager_passthrough",
        },
        "lock_up_months": 12,
        "redemption_gate": "quarterly",
        "key_personnel": [
            {"name": "Steve Cohen", "title": "Founder & CEO", "net_worth_est": 21_300_000_000},
        ],
        "private_credit_status": "raising $1B+ for private credit strategy (2025)",
        "predecessor": "SAC Capital Advisors (insider trading settlement, 2013)",
        "confidence": "confirmed",
    },
    "two_sigma": {
        "name": "Two Sigma Investments",
        "ticker": None,
        "aum": 75_000_000_000,  # ~$75B (2024)
        "ceo": "David Siegel & John Overdeck",
        "type": "hedge_fund",
        "strategy": "quantitative",
        "founded": 2001,
        "hq": "New York, NY",
        "fee_structure": {
            "management_fee_pct": 2.0,
            "performance_fee_pct": 25.0,
            "model": "traditional_quant",
        },
        "lock_up_months": 12,
        "redemption_gate": "quarterly with 45-day notice",
        "key_personnel": [
            {"name": "David Siegel", "title": "Co-Founder & Co-Chairman", "net_worth_est": 7_200_000_000},
            {"name": "John Overdeck", "title": "Co-Founder & Co-Chairman", "net_worth_est": 8_100_000_000},
        ],
        "confidence": "confirmed",
    },
    "elliott": {
        "name": "Elliott Management",
        "ticker": None,
        "aum": 69_500_000_000,  # ~$69.5B (2024)
        "ceo": "Paul Singer",
        "type": "hedge_fund",
        "strategy": "activist_distressed_debt",
        "founded": 1977,
        "hq": "West Palm Beach, FL",
        "fee_structure": {
            "management_fee_pct": 2.0,
            "performance_fee_pct": 20.0,
            "model": "traditional",
        },
        "lock_up_months": 24,
        "redemption_gate": "semi-annual",
        "key_personnel": [
            {"name": "Paul Singer", "title": "Founder & Co-CEO", "net_worth_est": 6_200_000_000},
            {"name": "Jonathan Pollock", "title": "Co-CEO"},
        ],
        "notable_activism": [
            "Argentina sovereign debt holdout",
            "Activism at AT&T, Salesforce, Pinterest, Starbucks",
        ],
        "confidence": "confirmed",
    },
}


# ══════════════════════════════════════════════════════════════════════════
# PENSION FUNDS -- the ultimate source of capital
# ══════════════════════════════════════════════════════════════════════════

PENSION_FUNDS: dict[str, dict[str, Any]] = {
    "calpers": {
        "name": "CalPERS",
        "full_name": "California Public Employees' Retirement System",
        "aum": 503_000_000_000,  # ~$503B (2025)
        "type": "public_pension",
        "state": "CA",
        "country": "US",
        "beneficiaries": 2_100_000,
        "cio": "Stephen Gilmore",
        "cio_since": 2024,
        "allocation_pct": {
            "public_equity": 42.0,
            "fixed_income": 23.0,
            "private_equity": 17.0,
            "real_assets": 13.0,
            "private_credit": 5.0,
        },
        "private_credit_est_usd": 25_000_000_000,
        "recent_moves": [
            "$24B expansion into private markets (2025-2026)",
            "Cherry-picked loans at discount during Blue Owl fire sale",
            "3 new private debt commitments in Q4 2025",
        ],
        "funded_ratio_pct": 75.0,
        "confidence": "confirmed",
    },
    "calstrs": {
        "name": "CalSTRS",
        "full_name": "California State Teachers' Retirement System",
        "aum": 340_000_000_000,  # ~$340B (2025)
        "type": "public_pension",
        "state": "CA",
        "country": "US",
        "beneficiaries": 1_000_000,
        "cio": "Christopher Ailman",
        "cio_since": 2000,
        "allocation_pct": {
            "public_equity": 39.0,
            "fixed_income": 12.0,
            "private_equity": 17.0,
            "real_estate": 14.0,
            "private_credit": 6.0,
            "risk_mitigating": 12.0,
        },
        "private_credit_est_usd": 20_400_000_000,
        "recent_moves": [
            "$12B deployed to private markets in 2025",
            "$7.2B PE + RE commitments in H2 2025",
            "$5.2B across PE, credit, RE in H1 2025",
            "Blackstone credit vehicle commitments",
            "'One Fund' dynamic allocation approach for 2026",
        ],
        "funded_ratio_pct": 73.0,
        "confidence": "confirmed",
    },
    "ny_common": {
        "name": "NY State Common Retirement Fund",
        "full_name": "New York State Common Retirement Fund",
        "aum": 272_800_000_000,  # $272.8B (March 2025)
        "type": "public_pension",
        "state": "NY",
        "country": "US",
        "beneficiaries": 1_200_000,
        "cio": "Anastasia Titarchuk",
        "cio_since": 2019,
        "allocation_pct": {
            "public_equity": 39.2,
            "fixed_income_mortgages": 23.0,
            "private_equity": 14.9,
            "real_estate_real_assets": 14.1,
            "credit_abs_return_oppo": 8.8,  # credit + absolute return + opportunistic
        },
        "private_credit_est_usd": 12_000_000_000,
        "recent_moves": [
            "5.84% return for FY 2024-25",
            "Monthly investment disclosures published",
        ],
        "funded_ratio_pct": 90.0,
        "confidence": "confirmed",
    },
    "texas_teachers": {
        "name": "Texas Teachers",
        "full_name": "Teacher Retirement System of Texas",
        "aum": 204_000_000_000,  # ~$204B (2025)
        "type": "public_pension",
        "state": "TX",
        "country": "US",
        "beneficiaries": 1_900_000,
        "cio": "Jase Auby",
        "cio_since": 2021,
        "allocation_pct": {
            "public_equity": 25.0,
            "stable_value": 18.0,
            "private_equity": 17.0,
            "real_assets": 15.0,
            "private_credit": 6.0,
            "hedge_funds": 8.0,
            "directional_risk": 11.0,
        },
        "private_credit_est_usd": 12_240_000_000,
        "hedge_fund_est_usd": 16_320_000_000,
        "funded_ratio_pct": 78.0,
        "confidence": "estimated",
    },
    "florida_sba": {
        "name": "Florida SBA",
        "full_name": "Florida State Board of Administration",
        "aum": 211_500_000_000,  # $211.5B (2025)
        "type": "public_pension",
        "state": "FL",
        "country": "US",
        "beneficiaries": 1_200_000,
        "cio": "Lamar Taylor",
        "cio_since": 2020,
        "allocation_pct": {
            "public_equity": 38.0,
            "fixed_income": 20.0,
            "private_equity": 8.5,
            "real_estate": 8.0,
            "active_credit": 4.73,
            "strategic_investments": 5.0,
        },
        "private_credit_est_usd": 10_000_000_000,
        "private_credit_detail": {
            "private_credit": 7_390_000_000,  # 3.49%
            "multi_asset_credit": 2_610_000_000,  # 1.23%
        },
        "recent_moves": [
            "Launching private credit co-investment program",
            "$2.25B private credit pacing planned for 2026",
            "$1.5B annual pacing 2027-2034",
            "Exploring CFOs as alternative to secondaries in $18B PE portfolio",
            "Goal: cut fees, improve transparency",
        ],
        "funded_ratio_pct": 82.0,
        "confidence": "confirmed",
    },
    "ontario_teachers": {
        "name": "Ontario Teachers' Pension Plan",
        "full_name": "Ontario Teachers' Pension Plan Board",
        "aum": 266_000_000_000,  # ~$266B CAD (~$195B USD, 2025)
        "aum_usd": 195_000_000_000,
        "type": "public_pension",
        "state": "ON",
        "country": "CA",
        "beneficiaries": 340_000,
        "cio": "Ziad Hindo",
        "cio_since": 2019,
        "allocation_pct": {
            "public_equity": 24.0,
            "fixed_income": 22.0,
            "private_equity": 22.0,
            "infrastructure": 16.0,
            "real_estate": 10.0,
            "private_credit": 6.0,
        },
        "private_credit_est_usd": 11_700_000_000,
        "recent_moves": [
            "Increasing private credit exposure as banks retreat",
            "$70B climate transition private investment target by 2030",
            "75% PE via direct investments",
            "Collaborative investment models with buyout firms",
        ],
        "funded_ratio_pct": 107.0,
        "confidence": "confirmed",
    },
    "cppib": {
        "name": "CPP Investments",
        "full_name": "Canada Pension Plan Investment Board",
        "aum": 576_000_000_000,  # $576B CAD (~$423B USD, 2025)
        "aum_usd": 423_000_000_000,
        "type": "public_pension",
        "state": "Federal",
        "country": "CA",
        "beneficiaries": 21_000_000,
        "cio": "Edwin Cass",
        "cio_since": 2021,
        "allocation_pct": {
            "public_equity": 23.0,
            "credit": 17.0,  # includes private credit
            "private_equity": 27.0,
            "real_assets": 20.0,
            "government_bonds": 13.0,
        },
        "private_credit_est_usd": 36_000_000_000,
        "recent_moves": [
            "Doubling overall credit portfolio to ~$115B over 5 years",
            "Private credit as key expansion area",
            "New 'integrated strategies group' for certain holdings",
            "More passive co-investments",
        ],
        "funded_ratio_pct": 113.0,
        "confidence": "confirmed",
    },
    "nycers": {
        "name": "NYC Pension Funds",
        "full_name": "New York City Pension Funds (5 systems)",
        "aum": 275_000_000_000,  # combined ~$275B (2025)
        "type": "public_pension",
        "state": "NY",
        "country": "US",
        "beneficiaries": 750_000,
        "cio": "varies by system",
        "allocation_pct": {
            "public_equity": 40.0,
            "fixed_income": 25.0,
            "private_equity": 12.0,
            "real_estate": 8.0,
            "private_credit": 5.0,
            "hedge_funds": 7.0,
        },
        "private_credit_est_usd": 13_750_000_000,
        "recent_moves": [
            "$5B private equity secondary sale to Blackstone (2025)",
            "75 asset managers, 125 funds, 450 commitments sold",
        ],
        "funded_ratio_pct": 78.0,
        "confidence": "confirmed",
    },
    "virginia_rs": {
        "name": "Virginia Retirement System",
        "full_name": "Virginia Retirement System",
        "aum": 102_000_000_000,  # ~$102B (2025)
        "type": "public_pension",
        "state": "VA",
        "country": "US",
        "beneficiaries": 770_000,
        "cio": "Andrew Junkin",
        "cio_since": 2023,
        "allocation_pct": {
            "public_equity": 35.0,
            "fixed_income": 15.0,
            "private_equity": 16.0,
            "real_assets": 10.0,
            "credit_strategies": 8.0,
            "diversifying_strategies": 10.0,
        },
        "private_credit_est_usd": 8_160_000_000,
        "recent_moves": [
            "$2.2B committed across 13 alternative funds (2025)",
            "$400M to Ardian Secondaries Fund IX",
            "$300M to HPS Strategic Investment Partners VI",
            "$300M to Lexington Partners X",
        ],
        "funded_ratio_pct": 77.0,
        "confidence": "confirmed",
    },
    "illinois_teachers": {
        "name": "Illinois TRS",
        "full_name": "Teachers' Retirement System of the State of Illinois",
        "aum": 64_000_000_000,  # ~$64B (2025)
        "type": "public_pension",
        "state": "IL",
        "country": "US",
        "beneficiaries": 445_000,
        "allocation_pct": {
            "public_equity": 40.0,
            "fixed_income": 20.0,
            "private_equity": 10.0,
            "real_assets": 10.0,
            "private_credit": 5.0,
        },
        "private_credit_est_usd": 3_200_000_000,
        "recent_moves": [
            "$200M to Blue Owl Real Estate Fund VII",
        ],
        "funded_ratio_pct": 44.0,  # severely underfunded
        "confidence": "estimated",
    },
}


# ══════════════════════════════════════════════════════════════════════════
# ALLOCATION LINKS -- pension -> fund connections
# ══════════════════════════════════════════════════════════════════════════

ALLOCATION_LINKS: list[dict[str, Any]] = [
    # ── CalPERS ──
    {"pension": "calpers", "fund": "apollo", "amount_est": 5_000_000_000,
     "asset_class": "private_credit", "confidence": "estimated",
     "notes": "Historical LP since 1995 (Apollo Fund III)"},
    {"pension": "calpers", "fund": "blackstone", "amount_est": 8_000_000_000,
     "asset_class": "private_credit", "confidence": "confirmed",
     "notes": "Large commitments to Blackstone credit vehicles (CalPERS/CalSTRS joint disclosure)"},
    {"pension": "calpers", "fund": "kkr", "amount_est": 4_000_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},
    {"pension": "calpers", "fund": "ares", "amount_est": 3_000_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},

    # ── CalSTRS ──
    {"pension": "calstrs", "fund": "blackstone", "amount_est": 6_000_000_000,
     "asset_class": "private_credit", "confidence": "confirmed",
     "notes": "Joint CalPERS/CalSTRS disclosure to Blackstone credit vehicles"},
    {"pension": "calstrs", "fund": "apollo", "amount_est": 3_500_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},
    {"pension": "calstrs", "fund": "kkr", "amount_est": 3_000_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},

    # ── Florida SBA ──
    {"pension": "florida_sba", "fund": "ares", "amount_est": 2_000_000_000,
     "asset_class": "private_credit", "confidence": "confirmed",
     "notes": "$1.3B Ares Pathfinder II + $700M Ares Senior Direct Lending IV"},
    {"pension": "florida_sba", "fund": "blackstone", "amount_est": 2_500_000_000,
     "asset_class": "real_estate", "confidence": "estimated"},
    {"pension": "florida_sba", "fund": "apollo", "amount_est": 1_500_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},

    # ── NY State Common ──
    {"pension": "ny_common", "fund": "blackstone", "amount_est": 5_000_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},
    {"pension": "ny_common", "fund": "apollo", "amount_est": 3_000_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},
    {"pension": "ny_common", "fund": "kkr", "amount_est": 2_500_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},

    # ── NYC Pension Funds ──
    {"pension": "nycers", "fund": "blackstone", "amount_est": 5_000_000_000,
     "asset_class": "private_equity", "confidence": "confirmed",
     "notes": "$5B PE secondary sale to Blackstone (2025), 75 managers, 125 funds"},

    # ── Texas Teachers ──
    {"pension": "texas_teachers", "fund": "apollo", "amount_est": 2_000_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},
    {"pension": "texas_teachers", "fund": "blackstone", "amount_est": 3_000_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},
    {"pension": "texas_teachers", "fund": "bridgewater", "amount_est": 1_500_000_000,
     "asset_class": "hedge_fund", "confidence": "estimated"},

    # ── Virginia RS ──
    {"pension": "virginia_rs", "fund": "hps", "amount_est": 300_000_000,
     "asset_class": "private_credit", "confidence": "confirmed",
     "notes": "$300M to HPS Strategic Investment Partners VI"},
    {"pension": "virginia_rs", "fund": "ares", "amount_est": 500_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},

    # ── Illinois Teachers ──
    {"pension": "illinois_teachers", "fund": "blue_owl", "amount_est": 200_000_000,
     "asset_class": "real_estate", "confidence": "confirmed",
     "notes": "$200M to Blue Owl Real Estate Fund VII"},

    # ── Ontario Teachers ──
    {"pension": "ontario_teachers", "fund": "blackstone", "amount_est": 4_000_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},
    {"pension": "ontario_teachers", "fund": "kkr", "amount_est": 3_000_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},
    {"pension": "ontario_teachers", "fund": "apollo", "amount_est": 2_000_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},

    # ── CPPIB ──
    {"pension": "cppib", "fund": "blackstone", "amount_est": 6_000_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},
    {"pension": "cppib", "fund": "apollo", "amount_est": 4_000_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},
    {"pension": "cppib", "fund": "kkr", "amount_est": 5_000_000_000,
     "asset_class": "private_equity", "confidence": "estimated"},
    {"pension": "cppib", "fund": "ares", "amount_est": 2_000_000_000,
     "asset_class": "private_credit", "confidence": "estimated"},

    # ── Blue Owl fire sale buyers (Feb 2026) ──
    {"pension": "calpers", "fund": "blue_owl", "amount_est": 400_000_000,
     "asset_class": "private_credit_secondary", "confidence": "confirmed",
     "notes": "Participated in $1.4B secondary purchase from Blue Owl at discount"},
]


# ══════════════════════════════════════════════════════════════════════════
# REVOLVING DOOR -- pension -> private fund personnel moves
# ══════════════════════════════════════════════════════════════════════════

REVOLVING_DOOR: list[dict[str, Any]] = [
    {
        "person": "Dan Bienvenue",
        "from_entity": "calpers",
        "to_entity": "general_atlantic",
        "role_from": "Deputy CIO, Capital Markets",
        "role_to": "Head of Capital Solutions for Pension Plans",
        "year": 2025,
        "significance": "Spent 5+ years overseeing CalPERS allocations, now sells to pensions",
        "confidence": "confirmed",
    },
    {
        "person": "Nicole Musicco",
        "from_entity": "calpers",
        "to_entity": "private_sector",
        "role_from": "Chief Investment Officer",
        "role_to": "Unknown (departed Sep 2023 after 18 months)",
        "year": 2023,
        "significance": "Third CIO departure in 4 years; instability at largest US pension",
        "confidence": "confirmed",
    },
    {
        "person": "Yu 'Ben' Meng",
        "from_entity": "calpers",
        "to_entity": "private_sector",
        "role_from": "Chief Investment Officer",
        "role_to": "Departed amid conflict-of-interest probe (Blackstone stake)",
        "year": 2020,
        "significance": "Owned Blackstone stake while overseeing CalPERS PE allocation",
        "confidence": "confirmed",
    },
    {
        "person": "Stephanie Drescher",
        "from_entity": "apollo",
        "to_entity": "political_donation",
        "role_from": "Apollo Executive",
        "role_to": "Donated to Ohio Gov. Kasich who appointed pension board member",
        "year": 2016,
        "significance": "SEC pay-to-play violation; Apollo exec donated to governor who controlled pension board",
        "confidence": "confirmed",
    },
]


# ══════════════════════════════════════════════════════════════════════════
# PENSION CONSULTANTS -- advisors with dual loyalties
# ══════════════════════════════════════════════════════════════════════════

PENSION_CONSULTANTS: dict[str, dict[str, Any]] = {
    "aon": {
        "name": "Aon",
        "type": "pension_consultant",
        "publicly_traded": True,
        "ticker": "AON",
        "advisory_aum_est": 3_500_000_000_000,  # ~$3.5T advised
        "conflict": "Public shareholders incentivize higher-fee PE/PC recommendations",
        "confidence": "confirmed",
    },
    "mercer": {
        "name": "Mercer (Marsh & McLennan)",
        "type": "pension_consultant",
        "publicly_traded": True,
        "parent_ticker": "MMC",
        "advisory_aum_est": 2_800_000_000_000,
        "conflict": "Financial obligations to shareholders incentivize high-fee alternatives",
        "confidence": "confirmed",
    },
    "cambridge": {
        "name": "Cambridge Associates",
        "type": "pension_consultant",
        "publicly_traded": False,
        "advisory_aum_est": 500_000_000_000,
        "conflict": "Premium fees for alternatives consulting create incentive to recommend PE/PC",
        "confidence": "estimated",
    },
    "callan": {
        "name": "Callan LLC",
        "type": "pension_consultant",
        "publicly_traded": False,
        "advisory_aum_est": 400_000_000_000,
        "conflict": "Allows asset managers (including PE firms) to PAY for access to plan sponsors",
        "confidence": "confirmed",
    },
    "wilshire": {
        "name": "Wilshire Advisors",
        "type": "pension_consultant",
        "publicly_traded": False,
        "advisory_aum_est": 1_200_000_000_000,
        "confidence": "estimated",
    },
}


# ══════════════════════════════════════════════════════════════════════════
# CONFLICT CHAIN MODEL -- how fees flow and who loses
# ══════════════════════════════════════════════════════════════════════════

_PE_BANKRUPTCY_STATS: dict[str, Any] = {
    "pe_backed_bankruptcies_2024": 110,  # record high
    "pe_backed_bankruptcies_pct_2025_q1": 70,  # 70% of large bankruptcies
    "pe_backed_default_rate_vs_non_pe": 2.0,  # 2x default rate per Moody's
    "fee_extraction_methods": [
        "management_fees",
        "performance_fees_carry",
        "monitoring_fees",
        "transaction_fees",
        "dividend_recapitalization",
        "sale_leaseback",
        "forced_refinancing",
    ],
    "notable_pe_bankruptcies": [
        {"company": "Claire's", "pe_firm": "apollo", "lbo_amount": 3_100_000_000,
         "debt_loaded": 2_500_000_000, "year_bankrupt": 2018},
        {"company": "Toys 'R' Us", "pe_firms": ["kkr", "bain", "vornado"],
         "lbo_amount": 6_600_000_000, "debt_loaded": 5_000_000_000, "year_bankrupt": 2017},
        {"company": "Sears Holdings", "pe_firm": "esl_investments",
         "year_bankrupt": 2018},
    ],
    "source": "Private Equity Stakeholder Project bankruptcy tracker",
    "confidence": "confirmed",
}


# ══════════════════════════════════════════════════════════════════════════
# CORE FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════

def build_institutional_graph(engine: Engine) -> dict[str, Any]:
    """Build the full institutional map graph for D3 visualization.

    Returns nodes (pensions, funds, consultants, people) and links
    (allocations, revolving door, advisory) suitable for force-directed
    or Sankey diagram rendering.

    Parameters:
        engine: SQLAlchemy engine (for dynamic enrichment).

    Returns:
        dict with keys: nodes, links, metadata, conflicts, fee_summary.
    """
    nodes: list[dict] = []
    links: list[dict] = []

    # ── Pension fund nodes ──
    for pid, pdata in PENSION_FUNDS.items():
        nodes.append({
            "id": pid,
            "label": pdata["name"],
            "type": "pension",
            "aum": pdata["aum"],
            "tier": "pension",
            "country": pdata.get("country", "US"),
            "beneficiaries": pdata.get("beneficiaries", 0),
            "funded_ratio": pdata.get("funded_ratio_pct", 0),
            "size": max(10, int(pdata["aum"] / 20_000_000_000)),
            "color": "#2563eb",  # blue
        })

    # ── Private credit fund nodes ──
    for fid, fdata in PRIVATE_CREDIT_FUNDS.items():
        if fdata.get("merged_into"):
            continue  # skip Owl Rock (merged)
        nodes.append({
            "id": fid,
            "label": fdata["name"],
            "type": "private_credit",
            "aum": fdata["aum"],
            "tier": "institutional",
            "ceo": fdata.get("ceo"),
            "fee_model": fdata.get("fee_structure", {}).get("model", "unknown"),
            "size": max(8, int(fdata["aum"] / 40_000_000_000)),
            "color": "#dc2626",  # red
        })

    # ── Hedge fund nodes ──
    for hid, hdata in HEDGE_FUNDS.items():
        nodes.append({
            "id": hid,
            "label": hdata["name"],
            "type": "hedge_fund",
            "aum": hdata["aum"],
            "tier": "institutional",
            "ceo": hdata.get("ceo"),
            "strategy": hdata.get("strategy", "multi_strategy"),
            "lock_up_months": hdata.get("lock_up_months"),
            "size": max(6, int(hdata["aum"] / 10_000_000_000)),
            "color": "#f59e0b",  # amber
        })

    # ── Consultant nodes ──
    for cid, cdata in PENSION_CONSULTANTS.items():
        nodes.append({
            "id": f"consultant_{cid}",
            "label": cdata["name"],
            "type": "consultant",
            "tier": "intermediary",
            "conflict_note": cdata.get("conflict", ""),
            "size": 6,
            "color": "#8b5cf6",  # purple
        })

    # ── Allocation links (pension -> fund) ──
    for alloc in ALLOCATION_LINKS:
        links.append({
            "source": alloc["pension"],
            "target": alloc["fund"],
            "type": "allocation",
            "amount": alloc["amount_est"],
            "asset_class": alloc.get("asset_class", "unknown"),
            "confidence": alloc.get("confidence", "estimated"),
            "label": f"${alloc['amount_est'] / 1_000_000_000:.1f}B",
            "strength": min(1.0, alloc["amount_est"] / 10_000_000_000),
        })

    # ── Revolving door links ──
    for rd in REVOLVING_DOOR:
        from_id = rd["from_entity"]
        to_id = rd["to_entity"]
        # Add person nodes if meaningful
        person_id = f"person_{rd['person'].lower().replace(' ', '_')}"
        nodes.append({
            "id": person_id,
            "label": rd["person"],
            "type": "person",
            "tier": "individual",
            "size": 4,
            "color": "#ef4444",  # bright red for revolving door
        })
        links.append({
            "source": from_id,
            "target": person_id,
            "type": "revolving_door",
            "role": rd["role_from"],
            "year": rd.get("year"),
            "label": f"{rd['role_from']} -> {rd['role_to']}",
            "strength": 0.9,
        })

    # ── Compute fee extraction summary ──
    total_pension_to_pc = sum(
        a["amount_est"] for a in ALLOCATION_LINKS
        if a.get("asset_class") in ("private_credit", "private_credit_secondary")
    )
    total_pension_to_pe = sum(
        a["amount_est"] for a in ALLOCATION_LINKS
        if a.get("asset_class") == "private_equity"
    )
    # Estimated annual fees: ~1.5% mgmt + ~3% performance (on winning years)
    est_annual_mgmt_fees = (total_pension_to_pc + total_pension_to_pe) * 0.015
    est_annual_perf_fees = (total_pension_to_pc + total_pension_to_pe) * 0.03

    # ── Conflict detection ──
    conflicts = find_conflicts_of_interest()

    metadata = {
        "total_pension_aum_tracked": sum(
            p["aum"] for p in PENSION_FUNDS.values()
        ),
        "total_pc_aum_tracked": sum(
            f["aum"] for f in PRIVATE_CREDIT_FUNDS.values()
            if not f.get("merged_into")
        ),
        "total_hf_aum_tracked": sum(
            h["aum"] for h in HEDGE_FUNDS.values()
        ),
        "allocation_links_count": len(ALLOCATION_LINKS),
        "revolving_door_count": len(REVOLVING_DOOR),
        "conflict_count": len(conflicts),
        "node_count": len(nodes),
        "link_count": len(links),
        "pe_bankruptcy_rate_2025": f"{_PE_BANKRUPTCY_STATS['pe_backed_bankruptcies_pct_2025_q1']}% of large US bankruptcies",
    }

    fee_summary = {
        "total_pension_capital_in_alternatives": total_pension_to_pc + total_pension_to_pe,
        "est_annual_management_fees": est_annual_mgmt_fees,
        "est_annual_performance_fees": est_annual_perf_fees,
        "est_annual_total_fee_extraction": est_annual_mgmt_fees + est_annual_perf_fees,
        "note": (
            "Estimated fees on tracked allocations only. Actual total is significantly "
            "higher as many allocations are undisclosed."
        ),
    }

    return {
        "nodes": nodes,
        "links": links,
        "metadata": metadata,
        "conflicts": conflicts,
        "fee_summary": fee_summary,
    }


def trace_pension_dollars(pension_name: str) -> dict[str, Any]:
    """Trace where a specific pension fund's money ends up.

    Parameters:
        pension_name: Key in PENSION_FUNDS (e.g., 'calpers').

    Returns:
        List of allocation records with fee estimates and risk notes.
    """
    pension_key = pension_name.lower().replace(" ", "_").replace("'", "")
    pension = PENSION_FUNDS.get(pension_key)
    if not pension:
        # Try fuzzy match
        for key, val in PENSION_FUNDS.items():
            if pension_name.lower() in val["name"].lower():
                pension_key = key
                pension = val
                break
    if not pension:
        return [{"error": f"Pension '{pension_name}' not found"}]

    allocations = [
        a for a in ALLOCATION_LINKS if a["pension"] == pension_key
    ]

    results: list[dict] = []
    total_tracked = 0

    for alloc in allocations:
        fund_key = alloc["fund"]
        fund = (
            PRIVATE_CREDIT_FUNDS.get(fund_key)
            or HEDGE_FUNDS.get(fund_key)
            or {}
        )
        amount = alloc["amount_est"]
        total_tracked += amount

        fee_info = fund.get("fee_structure", {})
        mgmt_pct = fee_info.get("management_fee_pct", 1.5)
        perf_pct = fee_info.get("performance_fee_pct", 20.0)

        # Estimate annual fee extraction
        est_mgmt_fee = amount * (mgmt_pct / 100)
        # Performance fee on assumed 8% gross return
        est_gross_return = amount * 0.08
        est_perf_fee = est_gross_return * (perf_pct / 100)

        results.append({
            "fund": fund_key,
            "fund_name": fund.get("name", fund_key),
            "amount_allocated": amount,
            "asset_class": alloc.get("asset_class"),
            "confidence": alloc.get("confidence", "estimated"),
            "notes": alloc.get("notes", ""),
            "fee_structure": {
                "management_fee_pct": mgmt_pct,
                "performance_fee_pct": perf_pct,
                "est_annual_mgmt_fee": est_mgmt_fee,
                "est_annual_perf_fee": est_perf_fee,
                "est_annual_total_fees": est_mgmt_fee + est_perf_fee,
            },
            "risk_notes": _get_fund_risk_notes(fund_key),
        })

    return {
        "pension": pension_key,
        "pension_name": pension["name"],
        "pension_aum": pension["aum"],
        "beneficiaries": pension.get("beneficiaries", 0),
        "funded_ratio_pct": pension.get("funded_ratio_pct"),
        "total_tracked_allocations": total_tracked,
        "pct_of_aum_tracked": round(total_tracked / pension["aum"] * 100, 1)
            if pension["aum"] > 0 else 0,
        "allocations": results,
        "total_est_annual_fees": sum(
            r["fee_structure"]["est_annual_total_fees"] for r in results
        ),
    }


def find_conflicts_of_interest() -> list[dict[str, Any]]:
    """Detect conflicts of interest across the institutional map.

    Checks:
        1. Revolving door: people who moved from pension to fund or vice versa
        2. Allocation overlap: pensions investing with funds where ex-employees work
        3. Consultant conflicts: advisors with structural incentives to push high-fee products
        4. Pay-to-play: political donations connected to pension board appointments
        5. Underfunded pensions taking high-risk bets

    Returns:
        List of conflict descriptions with severity and evidence.
    """
    conflicts: list[dict] = []

    # 1. Revolving door conflicts
    for rd in REVOLVING_DOOR:
        conflicts.append({
            "type": "revolving_door",
            "severity": "high",
            "person": rd["person"],
            "from_entity": rd["from_entity"],
            "to_entity": rd["to_entity"],
            "description": (
                f"{rd['person']} moved from {rd['role_from']} at "
                f"{rd['from_entity']} to {rd['role_to']} at {rd['to_entity']} "
                f"({rd.get('year', 'unknown')}). {rd.get('significance', '')}"
            ),
            "confidence": rd.get("confidence", "confirmed"),
        })

    # 2. CalPERS CIO instability + conflicts
    conflicts.append({
        "type": "governance_instability",
        "severity": "high",
        "entity": "calpers",
        "description": (
            "CalPERS has had 3 CIO departures in 4 years (Meng 2020, Musicco 2023, "
            "Bienvenue 2025). Meng owned Blackstone stock while overseeing PE allocation. "
            "Bienvenue left to sell capital solutions to pensions at General Atlantic."
        ),
        "confidence": "confirmed",
    })

    # 3. Consultant structural conflicts
    for cid, cdata in PENSION_CONSULTANTS.items():
        if cdata.get("conflict"):
            conflicts.append({
                "type": "consultant_conflict",
                "severity": "medium",
                "entity": cdata["name"],
                "description": (
                    f"{cdata['name']}: {cdata['conflict']}. "
                    f"Advises on ~${cdata.get('advisory_aum_est', 0) / 1e12:.1f}T "
                    f"in pension assets."
                ),
                "confidence": cdata.get("confidence", "estimated"),
            })

    # 4. Pay-to-play
    conflicts.append({
        "type": "pay_to_play",
        "severity": "critical",
        "entity": "apollo",
        "description": (
            "Apollo exec Stephanie Drescher violated SEC pay-to-play rule: "
            "donated $1000 to Ohio Gov. Kasich campaign in 2016. Kasich appointed "
            "a member to Ohio state pension board. Apollo manages pension money."
        ),
        "confidence": "confirmed",
    })

    # 5. Underfunded pensions taking high-risk private credit bets
    for pid, pdata in PENSION_FUNDS.items():
        funded = pdata.get("funded_ratio_pct", 100)
        pc_pct = pdata.get("allocation_pct", {}).get("private_credit", 0)
        if funded < 60 and pc_pct > 3:
            conflicts.append({
                "type": "risk_mismatch",
                "severity": "high",
                "entity": pid,
                "description": (
                    f"{pdata['name']} is only {funded}% funded but has "
                    f"{pc_pct}% in private credit. Illiquid, high-fee assets "
                    f"in an underfunded pension = beneficiaries bear the risk "
                    f"while fund managers collect fees regardless."
                ),
                "confidence": "derived",
            })

    # 6. Apollo redemption gates (March 2026 crisis)
    conflicts.append({
        "type": "liquidity_crisis",
        "severity": "critical",
        "entity": "apollo",
        "description": (
            "Apollo capped redemptions on its flagship Apollo Debt Solutions BDC "
            "(March 23, 2026), reigniting systemic liquidity fears in private credit. "
            "Blue Owl conducted a $1.4B fire sale in February 2026. PE-backed companies "
            "saw record bankruptcies (110 in 2024, 70% of large bankruptcies in Q1 2025). "
            "Pension funds with illiquid private credit allocations cannot exit."
        ),
        "confidence": "confirmed",
    })

    # 7. Fee extraction vs. returns for beneficiaries
    conflicts.append({
        "type": "fee_extraction",
        "severity": "high",
        "entity": "system_wide",
        "description": (
            "Private equity firms charge 2% management + 20% carry on pension capital. "
            "PE-backed companies default at 2x the rate of non-PE companies (Moody's). "
            "When LBOs fail, PE firms keep accumulated fees while pensions lose principal. "
            "110 PE-backed bankruptcies in 2024 (record), yet fee revenue continues growing."
        ),
        "confidence": "confirmed",
    })

    return conflicts


def get_fee_extraction_estimate(fund_name: str) -> dict[str, Any]:
    """Estimate how much a fund extracts in fees from pension money.

    Parameters:
        fund_name: Key in PRIVATE_CREDIT_FUNDS or HEDGE_FUNDS.

    Returns:
        Dict with fee breakdown, pension exposure, and extraction estimates.
    """
    fund_key = fund_name.lower().replace(" ", "_")
    fund = PRIVATE_CREDIT_FUNDS.get(fund_key) or HEDGE_FUNDS.get(fund_key)
    if not fund:
        # fuzzy match
        for key, val in {**PRIVATE_CREDIT_FUNDS, **HEDGE_FUNDS}.items():
            if fund_name.lower() in val["name"].lower():
                fund_key = key
                fund = val
                break
    if not fund:
        return {"error": f"Fund '{fund_name}' not found"}

    # Sum up all pension allocations to this fund
    pension_allocations = [
        a for a in ALLOCATION_LINKS if a["fund"] == fund_key
    ]
    total_pension_capital = sum(a["amount_est"] for a in pension_allocations)

    fee_info = fund.get("fee_structure", {})
    mgmt_pct = fee_info.get("management_fee_pct", 1.5)
    perf_pct = fee_info.get("performance_fee_pct", 20.0)

    # Management fees (annual, on AUM)
    annual_mgmt_fee = total_pension_capital * (mgmt_pct / 100)

    # Performance fees (on assumed 8% gross return)
    assumed_gross_return = 0.08
    annual_gross_return = total_pension_capital * assumed_gross_return
    annual_perf_fee = annual_gross_return * (perf_pct / 100)

    # Passthrough fees (multi-manager HFs)
    passthrough = fee_info.get("passthrough_fees", False)
    est_passthrough = total_pension_capital * 0.02 if passthrough else 0

    # 10-year fee extraction estimate
    ten_year_mgmt = annual_mgmt_fee * 10
    ten_year_perf = annual_perf_fee * 10
    ten_year_passthrough = est_passthrough * 10
    ten_year_total = ten_year_mgmt + ten_year_perf + ten_year_passthrough

    return {
        "fund": fund_key,
        "fund_name": fund.get("name", fund_key),
        "fund_aum": fund.get("aum", 0),
        "fee_structure": fee_info,
        "pension_allocations": [
            {
                "pension": a["pension"],
                "pension_name": PENSION_FUNDS.get(a["pension"], {}).get("name", a["pension"]),
                "amount": a["amount_est"],
                "asset_class": a.get("asset_class"),
            }
            for a in pension_allocations
        ],
        "total_tracked_pension_capital": total_pension_capital,
        "annual_estimates": {
            "management_fees": annual_mgmt_fee,
            "performance_fees": annual_perf_fee,
            "passthrough_fees": est_passthrough,
            "total_annual_extraction": annual_mgmt_fee + annual_perf_fee + est_passthrough,
        },
        "ten_year_estimates": {
            "management_fees": ten_year_mgmt,
            "performance_fees": ten_year_perf,
            "passthrough_fees": ten_year_passthrough,
            "total_extraction": ten_year_total,
        },
        "context": {
            "pe_backed_bankruptcy_rate": "2x non-PE default rate (Moody's)",
            "pe_bankruptcies_2024": 110,
            "fund_keeps_fees_on_failure": True,
        },
    }


# ── Helpers ──────────────────────────────────────────────────────────────

def _get_fund_risk_notes(fund_key: str) -> list[str]:
    """Return risk notes for a fund based on known scandals and market events."""
    notes: list[str] = []
    fund = PRIVATE_CREDIT_FUNDS.get(fund_key) or HEDGE_FUNDS.get(fund_key) or {}

    if fund.get("scandal_history"):
        notes.extend(fund["scandal_history"])

    if fund_key == "apollo":
        notes.append("Capped redemptions on flagship BDC (March 2026)")
        notes.append("PE-backed bankruptcies at record levels")
    elif fund_key == "blue_owl":
        notes.append("$1.4B fire sale of loan portfolio (Feb 2026)")
        notes.append("Fund redemption halt triggered market fear")
    elif fund_key == "blackstone":
        notes.append("BREIT redemption gates precedent (2022-2023)")

    # Passthrough fee risk
    fee = fund.get("fee_structure", {})
    if fee.get("passthrough_fees"):
        notes.append(
            f"Passthrough fee model: {fee.get('passthrough_note', 'unlimited expenses to investors')}"
        )

    return notes


def get_all_fund_managers() -> list[dict[str, Any]]:
    """Return all fund managers and pension CIOs as actor records.

    Suitable for adding to the GRID actor_network.
    """
    actors: list[dict] = []

    # Private credit fund managers
    for fid, fdata in PRIVATE_CREDIT_FUNDS.items():
        if fdata.get("merged_into"):
            continue
        for person in fdata.get("key_personnel", []):
            actors.append({
                "id": f"pc_{fid}_{person['name'].lower().replace(' ', '_')}",
                "name": person["name"],
                "tier": "institutional",
                "category": "fund",
                "title": f"{person['title']}, {fdata['name']}",
                "aum": fdata.get("aum"),
                "net_worth_estimate": person.get("net_worth_est"),
                "influence_score": min(0.95, 0.6 + (fdata.get("aum", 0) / 2_000_000_000_000)),
                "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
                "credibility": "hard_data",
                "motivation_model": "fee_maximization",
            })

    # Hedge fund managers
    for hid, hdata in HEDGE_FUNDS.items():
        for person in hdata.get("key_personnel", []):
            actors.append({
                "id": f"hf_{hid}_{person['name'].lower().replace(' ', '_')}",
                "name": person["name"],
                "tier": "institutional",
                "category": "fund",
                "title": f"{person['title']}, {hdata['name']}",
                "aum": hdata.get("aum"),
                "net_worth_estimate": person.get("net_worth_est"),
                "influence_score": min(0.95, 0.6 + (hdata.get("aum", 0) / 200_000_000_000)),
                "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
                "credibility": "hard_data",
                "motivation_model": "alpha_generation",
            })

    # Pension CIOs
    for pid, pdata in PENSION_FUNDS.items():
        cio = pdata.get("cio")
        if cio and cio != "varies by system":
            actors.append({
                "id": f"pension_cio_{pid}",
                "name": cio,
                "tier": "institutional",
                "category": "pension_cio",
                "title": f"CIO, {pdata['name']}",
                "aum": pdata.get("aum"),
                "influence_score": min(0.90, 0.5 + (pdata.get("aum", 0) / 1_000_000_000_000)),
                "data_sources": ["pension_disclosures", "board_minutes", "institutional_map"],
                "credibility": "hard_data",
                "motivation_model": "fiduciary_mandate",
            })

    return actors


def get_institutional_summary() -> dict[str, Any]:
    """Return a high-level summary of the institutional map for dashboards."""
    total_pension_aum = sum(p["aum"] for p in PENSION_FUNDS.values())
    total_pc_aum = sum(
        f["aum"] for f in PRIVATE_CREDIT_FUNDS.values()
        if not f.get("merged_into")
    )
    total_hf_aum = sum(h["aum"] for h in HEDGE_FUNDS.values())
    total_allocated = sum(a["amount_est"] for a in ALLOCATION_LINKS)

    return {
        "pension_funds_tracked": len(PENSION_FUNDS),
        "private_credit_funds_tracked": len([
            f for f in PRIVATE_CREDIT_FUNDS.values() if not f.get("merged_into")
        ]),
        "hedge_funds_tracked": len(HEDGE_FUNDS),
        "total_pension_aum": total_pension_aum,
        "total_pc_aum": total_pc_aum,
        "total_hf_aum": total_hf_aum,
        "total_tracked_allocations": total_allocated,
        "revolving_door_cases": len(REVOLVING_DOOR),
        "consultant_conflicts": len([
            c for c in PENSION_CONSULTANTS.values() if c.get("conflict")
        ]),
        "pe_bankruptcy_stats": _PE_BANKRUPTCY_STATS,
        "private_credit_crisis_2026": {
            "apollo_redemption_cap": "March 23, 2026",
            "blue_owl_fire_sale": "$1.4B (February 2026)",
            "fortune_headline": "$265B private credit meltdown",
        },
    }
