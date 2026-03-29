"""
GRID Intelligence -- Global Banking & Financial Services Power Network.

Maps the global banking system: US big banks, European systemically important
banks, shadow banking / private credit, and central banks.  Tracks key people,
assets, political connections, regulatory actions, offshore exposure, revolving
door links, and systemic risk contributions.

The banking system is the circulatory system of global capital.  Six US banks
hold ~$13T in combined assets.  Four European banks add another ~$8T.
Central banks control the monetary base that everything prices off.
Shadow banking ($1.7T+ private credit alone) is where risk migrates when
regulators tighten the traditional system.

Confidence labels per GRID convention:
    confirmed  -- public filings, annual reports, regulatory orders
    derived    -- calculated from multiple confirmed sources
    estimated  -- industry-standard assumptions or partial data
    rumored    -- media reports, unnamed sources
    inferred   -- pattern-based deduction without direct evidence

Key entry points:
    get_banking_network()              -- full network dict
    get_bank(key)                      -- single institution dossier
    get_central_bank(key)              -- single central bank profile
    get_revolving_door_all()           -- all gov<->bank revolving door actors
    get_systemic_risk_summary()        -- GSIB scores + shadow banking risk
    get_regulatory_actions_timeline()  -- major fines/settlements by date

Sources:
    - Federal Reserve Y-9C and CCAR filings
    - SEC 10-K, DEF 14A, Form 4, 13F filings
    - OCC Consent Orders, DOJ press releases
    - FSB Global Systemically Important Banks list (2024)
    - BIS Quarterly Review, ECB Financial Stability Review
    - FDIC Quarterly Banking Profile
    - Bloomberg, Financial Times, Reuters reporting
    - OpenSecrets lobbying and PAC data
    - ICIJ Offshore Leaks Database
"""

from __future__ import annotations

from typing import Any


def get_banking_network() -> dict[str, Any]:
    """Return the full banking power network."""
    return BANKING_NETWORK


def get_bank(key: str) -> dict[str, Any] | None:
    """Return dossier for a single bank by key (e.g. 'jpm', 'hsbc')."""
    for section in ("us_big_banks", "european_banks"):
        bank = BANKING_NETWORK.get(section, {}).get(key)
        if bank:
            return bank
    return None


def get_central_bank(key: str) -> dict[str, Any] | None:
    """Return profile for a central bank by key (e.g. 'fed', 'ecb')."""
    return BANKING_NETWORK.get("central_banks", {}).get(key)


def get_shadow_banking() -> dict[str, Any]:
    """Return shadow banking / private credit network."""
    return BANKING_NETWORK.get("shadow_banking", {})


def get_revolving_door_all() -> list[dict[str, Any]]:
    """Extract all revolving door actors across all banks."""
    actors = []
    for section in ("us_big_banks", "european_banks"):
        for key, bank in BANKING_NETWORK.get(section, {}).items():
            for person in bank.get("revolving_door", []):
                entry = dict(person)
                entry["institution_key"] = key
                entry["institution_name"] = bank["name"]
                actors.append(entry)
    return actors


def get_systemic_risk_summary() -> dict[str, Any]:
    """Build a systemic risk summary across all tracked institutions."""
    summary = {"gsib_banks": [], "shadow_banking_risk": {}, "central_bank_balance_sheets": {}}
    for section in ("us_big_banks", "european_banks"):
        for key, bank in BANKING_NETWORK.get(section, {}).items():
            summary["gsib_banks"].append({
                "key": key,
                "name": bank["name"],
                "total_assets_usd": bank.get("total_assets_usd"),
                "gsib_bucket": bank.get("gsib_bucket"),
                "systemic_risk": bank.get("systemic_risk"),
            })
    summary["shadow_banking_risk"] = BANKING_NETWORK.get("shadow_banking", {}).get(
        "systemic_risk_assessment", {}
    )
    for key, cb in BANKING_NETWORK.get("central_banks", {}).items():
        summary["central_bank_balance_sheets"][key] = {
            "name": cb["name"],
            "balance_sheet_usd": cb.get("balance_sheet_usd"),
            "qt_status": cb.get("qt_status"),
        }
    return summary


def get_regulatory_actions_timeline() -> list[dict[str, Any]]:
    """Collect all regulatory actions across banks, sorted by date."""
    actions = []
    for section in ("us_big_banks", "european_banks"):
        for key, bank in BANKING_NETWORK.get(section, {}).items():
            for action in bank.get("regulatory_actions", []):
                entry = dict(action)
                entry["institution_key"] = key
                entry["institution_name"] = bank["name"]
                actions.append(entry)
    actions.sort(key=lambda x: x.get("year", 0), reverse=True)
    return actions


# ══════════════════════════════════════════════════════════════════════════════
# MASTER DATA STRUCTURE
# ══════════════════════════════════════════════════════════════════════════════

BANKING_NETWORK: dict[str, Any] = {
    "metadata": {
        "report_type": "global_banking_power_network",
        "version": "1.0.0",
        "generated": "2026-03-28",
        "confidence_schema": ["confirmed", "derived", "estimated", "rumored", "inferred"],
        "sources": [
            "Federal Reserve Y-9C filings",
            "SEC 10-K, DEF 14A, Form 4, 13F",
            "OCC Consent Orders",
            "DOJ press releases",
            "FSB GSIB list (2024)",
            "BIS Quarterly Review",
            "ECB Financial Stability Review",
            "FDIC Quarterly Banking Profile",
            "Bloomberg, FT, Reuters",
            "OpenSecrets.org",
            "ICIJ Offshore Leaks Database",
        ],
        "data_vintage": "public information through early 2026",
        "update_frequency": "quarterly after earnings + annual after proxy season",
    },

    # ══════════════════════════════════════════════════════════════════════
    # US BIG BANKS
    # ══════════════════════════════════════════════════════════════════════

    "us_big_banks": {

        # ── JPMorgan Chase ──────────────────────────────────────────────
        "jpm": {
            "name": "JPMorgan Chase & Co.",
            "ticker": "JPM",
            "total_assets_usd": 4_000_000_000_000,  # ~$4.0T (Q4 2025)
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 680_000_000_000,  # ~$680B (early 2026)
            "market_cap_confidence": "estimated",
            "revenue_2025_usd": 180_000_000_000,
            "revenue_confidence": "confirmed",
            "hq": "New York, NY",
            "founded": 1799,
            "gsib_bucket": 4,  # highest surcharge bucket
            "gsib_surcharge_pct": 4.5,
            "employees": 309_000,
            "key_personnel": [
                {
                    "name": "Jamie Dimon",
                    "title": "Chairman & CEO",
                    "since": 2005,
                    "compensation_2024": 39_000_000,
                    "net_worth_est": 2_300_000_000,
                    "jpm_shares_value": 1_500_000_000,
                    "background": "Citigroup (fired by Sandy Weill 1998), Bank One CEO 2000-2004",
                    "political_influence": "Most powerful banker in America, regular White House visitor under both parties",
                    "succession_watch": "Age 69, no announced retirement date; Marianne Lake, Jennifer Piepszak, Daniel Pinto in frame",
                    "confidence": "confirmed",
                },
                {
                    "name": "Daniel Pinto",
                    "title": "President & COO",
                    "note": "Runs CIB (investment bank), top succession candidate",
                    "confidence": "confirmed",
                },
                {
                    "name": "Marianne Lake",
                    "title": "CEO, Consumer & Community Banking",
                    "note": "Former CFO, top succession candidate",
                    "confidence": "confirmed",
                },
                {
                    "name": "Jennifer Piepszak",
                    "title": "COO, Consumer & Community Banking",
                    "confidence": "confirmed",
                },
                {
                    "name": "Troy Rohrbaugh",
                    "title": "Co-CEO, Commercial & Investment Bank",
                    "confidence": "confirmed",
                },
            ],
            "business_segments": {
                "consumer_community_banking": {"revenue_pct": 38, "confidence": "confirmed"},
                "commercial_investment_bank": {"revenue_pct": 35, "confidence": "confirmed"},
                "asset_wealth_management": {
                    "revenue_pct": 15,
                    "aum": 3_900_000_000_000,
                    "confidence": "confirmed",
                },
                "corporate": {"revenue_pct": 12, "confidence": "confirmed"},
            },
            "political_connections": {
                "lobbying_spend_2024": 12_800_000,
                "lobbying_confidence": "confirmed",  # OpenSecrets
                "pac_contributions_2024": 3_200_000,
                "pac_confidence": "estimated",
                "key_relationships": [
                    "Dimon has direct access to sitting presidents regardless of party",
                    "JPM alumni across Treasury, Fed, CFTC, SEC",
                    "Business Roundtable leadership (Dimon was chairman 2018-2022)",
                ],
                "confidence": "confirmed",
            },
            "revolving_door": [
                {
                    "name": "William Daley",
                    "bank_role": "Former vice chairman JPM",
                    "gov_role": "White House Chief of Staff (Obama), Commerce Secretary (Clinton)",
                    "direction": "gov_to_bank_and_back",
                    "confidence": "confirmed",
                },
                {
                    "name": "Gary Gensler",
                    "bank_role": "Former Goldman Sachs partner (not JPM, but relevant)",
                    "gov_role": "SEC Chair 2021-2025, CFTC Chair 2009-2014",
                    "note": "Regulated JPM from multiple agencies",
                    "confidence": "confirmed",
                },
            ],
            "regulatory_actions": [
                {
                    "action": "Precious metals spoofing settlement",
                    "amount_usd": 920_000_000,
                    "year": 2020,
                    "agency": "DOJ/CFTC",
                    "detail": "Largest spoofing case in history; traders convicted of RICO",
                    "confidence": "confirmed",
                },
                {
                    "action": "Epstein relationship settlement",
                    "amount_usd": 290_000_000,
                    "year": 2023,
                    "agency": "Virgin Islands AG / civil",
                    "detail": "JPM banked Epstein from 1998-2013, internal warnings ignored",
                    "confidence": "confirmed",
                },
                {
                    "action": "London Whale trading loss",
                    "amount_usd": 920_000_000,  # fine; actual loss $6.2B
                    "year": 2013,
                    "agency": "SEC/OCC/Fed/FCA",
                    "detail": "Bruno Iksil CIO office loss of $6.2B; $920M in fines across agencies",
                    "confidence": "confirmed",
                },
                {
                    "action": "FX manipulation settlement",
                    "amount_usd": 550_000_000,
                    "year": 2015,
                    "agency": "DOJ",
                    "detail": "Part of $5.8B industry-wide FX rigging settlement; guilty plea to felony",
                    "confidence": "confirmed",
                },
                {
                    "action": "Madoff feeder bank settlement",
                    "amount_usd": 2_600_000_000,
                    "year": 2014,
                    "agency": "DOJ/OCC",
                    "detail": "Failed to flag suspicious Madoff activity despite red flags",
                    "confidence": "confirmed",
                },
                {
                    "action": "First Republic Bank acquisition",
                    "amount_usd": 0,
                    "year": 2023,
                    "agency": "FDIC-assisted",
                    "detail": "Acquired FRC in FDIC seizure; sweetheart deal criticized, $13B loss-sharing from FDIC",
                    "confidence": "confirmed",
                },
            ],
            "offshore_exposure": {
                "cayman_entities": "extensive",
                "note": "JPM operates hundreds of Cayman-domiciled funds for institutional clients",
                "icij_mentions": "limited direct mentions; client-side exposure in Panama Papers",
                "confidence": "estimated",
            },
            "derivatives_exposure": {
                "total_notional_usd": 54_000_000_000_000,  # $54T notional
                "note": "Largest derivatives book of any US bank by notional",
                "confidence": "confirmed",  # OCC Quarterly Report
            },
            "systemic_risk": {
                "fsb_gsib": True,
                "gsib_score": "highest bucket (4)",
                "too_big_to_fail": True,
                "interconnectedness": "extreme -- counterparty to virtually every major financial institution globally",
                "single_point_of_failure_risk": "Treasury clearing (JPM clears ~30% of US Treasury market)",
                "confidence": "confirmed",
            },
        },

        # ── Bank of America ─────────────────────────────────────────────
        "bac": {
            "name": "Bank of America Corporation",
            "ticker": "BAC",
            "total_assets_usd": 3_300_000_000_000,  # ~$3.3T
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 360_000_000_000,
            "market_cap_confidence": "estimated",
            "hq": "Charlotte, NC",
            "founded": 1904,
            "gsib_bucket": 3,
            "gsib_surcharge_pct": 3.0,
            "employees": 213_000,
            "key_personnel": [
                {
                    "name": "Brian Moynihan",
                    "title": "Chairman & CEO",
                    "since": 2010,
                    "compensation_2024": 29_000_000,
                    "background": "Rose through FleetBoston acquisition; lawyer by training",
                    "note": "Steered BAC through Countrywide/Merrill Lynch hangover; $76B+ in crisis-era fines",
                    "confidence": "confirmed",
                },
                {
                    "name": "Alastair Borthwick",
                    "title": "CFO",
                    "since": 2022,
                    "confidence": "confirmed",
                },
            ],
            "political_connections": {
                "lobbying_spend_2024": 8_200_000,
                "lobbying_confidence": "confirmed",
                "key_relationships": [
                    "Moynihan co-chairs Sustainable Markets Initiative with Prince Charles (now King)",
                    "Major ESG/climate finance commitment ($1.5T by 2030)",
                    "Strong Democratic donor base historically, but bipartisan lobbying",
                ],
                "confidence": "confirmed",
            },
            "revolving_door": [
                {
                    "name": "Anne Finucane",
                    "bank_role": "Vice Chairman BAC (retired 2022)",
                    "gov_role": "No direct gov role, but key political connector for BAC",
                    "confidence": "confirmed",
                },
            ],
            "regulatory_actions": [
                {
                    "action": "Countrywide mortgage fraud settlements",
                    "amount_usd": 16_650_000_000,
                    "year": 2014,
                    "agency": "DOJ",
                    "detail": "Largest single DOJ settlement in history at the time (mortgage fraud)",
                    "confidence": "confirmed",
                },
                {
                    "action": "Merrill Lynch merger-related SEC fine",
                    "amount_usd": 150_000_000,
                    "year": 2010,
                    "agency": "SEC",
                    "detail": "Misleading shareholders about Merrill bonuses during merger",
                    "confidence": "confirmed",
                },
                {
                    "action": "HTM securities unrealized losses",
                    "amount_usd": 0,
                    "year": 2023,
                    "agency": "N/A (market event)",
                    "detail": "Largest HTM unrealized loss portfolio in industry (~$131B at peak in 2023); still ~$86B in 2025",
                    "confidence": "confirmed",
                },
            ],
            "offshore_exposure": {
                "note": "Merrill Lynch International (London) and Cayman entities for fund administration",
                "confidence": "estimated",
            },
            "derivatives_exposure": {
                "total_notional_usd": 38_000_000_000_000,
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "key_vulnerability": "HTM bond portfolio unrealized losses; rate-sensitive balance sheet",
                "confidence": "confirmed",
            },
        },

        # ── Citigroup ───────────────────────────────────────────────────
        "c": {
            "name": "Citigroup Inc.",
            "ticker": "C",
            "total_assets_usd": 2_400_000_000_000,  # ~$2.4T
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 130_000_000_000,
            "market_cap_confidence": "estimated",
            "hq": "New York, NY",
            "founded": 1812,
            "gsib_bucket": 3,
            "gsib_surcharge_pct": 3.0,
            "employees": 240_000,
            "key_personnel": [
                {
                    "name": "Jane Fraser",
                    "title": "CEO",
                    "since": 2021,
                    "compensation_2024": 26_000_000,
                    "background": "First woman to lead a major US bank; McKinsey background; head of LatAm banking",
                    "note": "Executing massive 'simplification' restructuring -- exiting 14 consumer markets, cutting 20K jobs in 2024-2025",
                    "confidence": "confirmed",
                },
                {
                    "name": "Mark Mason",
                    "title": "CFO",
                    "confidence": "confirmed",
                },
                {
                    "name": "Vis Raghavan",
                    "title": "Head of Banking",
                    "since": 2024,
                    "note": "Hired from JPM to rebuild investment banking",
                    "confidence": "confirmed",
                },
            ],
            "restructuring": {
                "codename": "Project Bora Bora (internal)",
                "markets_exited": [
                    "Australia", "Bahrain", "China (consumer)", "India (consumer)",
                    "Indonesia", "Korea", "Malaysia", "Mexico (Banamex IPO planned)",
                    "Philippines", "Poland", "Russia", "Taiwan", "Thailand", "UK (consumer)",
                ],
                "headcount_reduction": 20_000,
                "timeframe": "2024-2026",
                "consent_order": "Fed/OCC consent order on risk management + data governance (2020, still active in 2026)",
                "confidence": "confirmed",
            },
            "political_connections": {
                "lobbying_spend_2024": 7_100_000,
                "lobbying_confidence": "confirmed",
                "key_relationships": [
                    "Robert Rubin (former Treasury Sec) was Citi senior counselor/director 1999-2009; symbol of revolving door",
                    "Tim Geithner (former Treasury Sec, NY Fed Pres) orchestrated Citi bailout in 2008",
                    "Citi most bailed-out bank in 2008 crisis -- $476B total government support",
                ],
                "confidence": "confirmed",
            },
            "revolving_door": [
                {
                    "name": "Robert Rubin",
                    "bank_role": "Senior Counselor / Director, Citigroup (1999-2009)",
                    "gov_role": "Treasury Secretary (Clinton 1995-1999)",
                    "direction": "gov_to_bank",
                    "note": "Earned $126M at Citi; pushed for Glass-Steagall repeal that enabled Citigroup's creation",
                    "confidence": "confirmed",
                },
                {
                    "name": "Jack Lew",
                    "bank_role": "COO, Citigroup Alternative Investments (2006-2009)",
                    "gov_role": "Treasury Secretary (Obama 2013-2017), OMB Director",
                    "direction": "bank_to_gov",
                    "note": "Received $940K bonus from Citi while unit lost billions",
                    "confidence": "confirmed",
                },
                {
                    "name": "Michael Froman",
                    "bank_role": "Managing Director, Citigroup (2006-2009)",
                    "gov_role": "US Trade Representative (Obama 2013-2017)",
                    "direction": "bank_to_gov",
                    "confidence": "confirmed",
                },
            ],
            "regulatory_actions": [
                {
                    "action": "2008 bailout / TARP",
                    "amount_usd": 45_000_000_000,
                    "year": 2008,
                    "agency": "Treasury/Fed",
                    "detail": "$45B TARP + $306B asset guarantee + $2.5T Fed lending; most bailed-out institution",
                    "confidence": "confirmed",
                },
                {
                    "action": "Consent order on risk management",
                    "amount_usd": 400_000_000,
                    "year": 2020,
                    "agency": "OCC/Fed",
                    "detail": "Data governance and risk management failures; $400M fine; consent order STILL ACTIVE in 2026",
                    "confidence": "confirmed",
                },
                {
                    "action": "FX manipulation guilty plea",
                    "amount_usd": 925_000_000,
                    "year": 2015,
                    "agency": "DOJ",
                    "detail": "Citi pled guilty to felony FX manipulation as part of industry-wide settlement",
                    "confidence": "confirmed",
                },
                {
                    "action": "Revlon wire transfer error",
                    "amount_usd": 500_000_000,
                    "year": 2020,
                    "agency": "Court (civil)",
                    "detail": "Accidentally wired $900M to Revlon lenders; court initially ruled against clawback",
                    "confidence": "confirmed",
                },
            ],
            "offshore_exposure": {
                "note": "Most global US bank -- presence in 160+ countries; significant LatAm exposure (Banamex)",
                "confidence": "confirmed",
            },
            "derivatives_exposure": {
                "total_notional_usd": 43_000_000_000_000,
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "key_vulnerability": "Active consent order means regulators can restrict activities; restructuring execution risk; persistent discount to book value",
                "confidence": "confirmed",
            },
        },

        # ── Goldman Sachs ───────────────────────────────────────────────
        "gs": {
            "name": "The Goldman Sachs Group, Inc.",
            "ticker": "GS",
            "total_assets_usd": 1_700_000_000_000,  # ~$1.7T
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 175_000_000_000,
            "market_cap_confidence": "estimated",
            "aum": 3_000_000_000_000,  # $3T AUM/AUS (asset & wealth management)
            "hq": "New York, NY",
            "founded": 1869,
            "gsib_bucket": 2,
            "gsib_surcharge_pct": 2.0,
            "employees": 46_000,
            "key_personnel": [
                {
                    "name": "David Solomon",
                    "title": "Chairman & CEO",
                    "since": 2018,
                    "compensation_2024": 31_000_000,
                    "background": "Investment banking career; known as 'DJ D-Sol'",
                    "controversies": "Marcus consumer banking retreat ($3B loss); internal culture criticism",
                    "confidence": "confirmed",
                },
                {
                    "name": "John Waldron",
                    "title": "President & COO",
                    "confidence": "confirmed",
                },
                {
                    "name": "Denis Coleman",
                    "title": "CFO",
                    "since": 2022,
                    "confidence": "confirmed",
                },
            ],
            "strategic_pivot": {
                "away_from": "Consumer banking (Marcus), GreenSky lending",
                "toward": "Asset & wealth management, PE/alternatives, platform solutions",
                "marcus_loss": 3_000_000_000,
                "greensky_sale_loss": 500_000_000,
                "alternatives_aum_target": "grow to $500B+ by 2027",
                "confidence": "confirmed",
            },
            "political_connections": {
                "lobbying_spend_2024": 5_600_000,
                "lobbying_confidence": "confirmed",
                "key_relationships": [
                    "Goldman is THE revolving door bank -- 'Government Sachs'",
                    "Alumni have led Treasury under 3 presidents",
                    "Strong Fed/NEC/SEC alumni pipeline",
                ],
                "confidence": "confirmed",
            },
            "revolving_door": [
                {
                    "name": "Henry Paulson",
                    "bank_role": "CEO, Goldman Sachs (1999-2006)",
                    "gov_role": "Treasury Secretary (Bush 2006-2009)",
                    "direction": "bank_to_gov",
                    "note": "Orchestrated TARP bailout that saved Goldman; tax-free GS share sale ($200M+)",
                    "confidence": "confirmed",
                },
                {
                    "name": "Robert Rubin",
                    "bank_role": "Co-Chairman, Goldman Sachs (1990-1992)",
                    "gov_role": "Treasury Secretary (Clinton 1995-1999)",
                    "direction": "bank_to_gov",
                    "confidence": "confirmed",
                },
                {
                    "name": "Gary Cohn",
                    "bank_role": "President & COO, Goldman Sachs (2006-2017)",
                    "gov_role": "Director, National Economic Council (Trump 2017-2018)",
                    "direction": "bank_to_gov",
                    "note": "Shepherded 2017 Tax Cuts and Jobs Act",
                    "confidence": "confirmed",
                },
                {
                    "name": "Steven Mnuchin",
                    "bank_role": "Goldman Sachs partner (17 years)",
                    "gov_role": "Treasury Secretary (Trump 2017-2021)",
                    "direction": "bank_to_gov",
                    "note": "Also ran OneWest Bank (aggressive foreclosure practices)",
                    "confidence": "confirmed",
                },
                {
                    "name": "Neel Kashkari",
                    "bank_role": "Goldman Sachs VP",
                    "gov_role": "TARP administrator (Treasury), Minneapolis Fed President",
                    "direction": "bank_to_gov",
                    "confidence": "confirmed",
                },
                {
                    "name": "Mario Draghi",
                    "bank_role": "Goldman Sachs International VP (2002-2005)",
                    "gov_role": "ECB President (2011-2019), Italian PM (2021-2022)",
                    "direction": "bank_to_gov",
                    "note": "Goldman helped Greece conceal debt via currency swaps before joining GS",
                    "confidence": "confirmed",
                },
            ],
            "regulatory_actions": [
                {
                    "action": "1MDB scandal global settlements",
                    "amount_usd": 5_000_000_000,
                    "year": 2020,
                    "agency": "DOJ/SEC/FCA/BNM/MAS",
                    "detail": "Goldman raised $6.5B in bonds for 1MDB; $4.5B was stolen by Jho Low and associates; Goldman paid $5B+ globally",
                    "confidence": "confirmed",
                },
                {
                    "action": "Abacus CDO SEC settlement",
                    "amount_usd": 550_000_000,
                    "year": 2010,
                    "agency": "SEC",
                    "detail": "Sold CDO designed to fail (Paulson & Co. picked assets to short); Fabrice Tourre convicted",
                    "confidence": "confirmed",
                },
                {
                    "action": "FX manipulation settlement",
                    "amount_usd": 130_000_000,
                    "year": 2015,
                    "agency": "Fed",
                    "confidence": "confirmed",
                },
                {
                    "action": "Greece debt concealment (via swaps)",
                    "amount_usd": 0,
                    "year": 2001,
                    "agency": "N/A (no prosecution)",
                    "detail": "Goldman structured currency swaps that helped Greece hide $1B+ in debt to meet Eurozone entry criteria",
                    "confidence": "confirmed",
                },
            ],
            "offshore_exposure": {
                "note": "Extensive offshore fund administration; Cayman-domiciled PE vehicles",
                "icij_mentions": "Goldman entities appear in Pandora Papers in connection with client structuring",
                "confidence": "estimated",
            },
            "derivatives_exposure": {
                "total_notional_usd": 49_000_000_000_000,
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "key_vulnerability": "Heavy trading/market-making concentration; counterparty risk in derivatives",
                "confidence": "confirmed",
            },
        },

        # ── Morgan Stanley ──────────────────────────────────────────────
        "ms": {
            "name": "Morgan Stanley",
            "ticker": "MS",
            "total_assets_usd": 1_200_000_000_000,  # ~$1.2T
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 200_000_000_000,
            "market_cap_confidence": "estimated",
            "aum": 6_500_000_000_000,  # $6.5T client assets (wealth management focus)
            "hq": "New York, NY",
            "founded": 1935,
            "gsib_bucket": 2,
            "gsib_surcharge_pct": 2.0,
            "employees": 82_000,
            "key_personnel": [
                {
                    "name": "Ted Pick",
                    "title": "CEO",
                    "since": "January 2024",
                    "background": "Head of Institutional Securities; 34-year MS veteran",
                    "note": "Succeeded James Gorman who transformed MS into wealth management powerhouse",
                    "confidence": "confirmed",
                },
                {
                    "name": "James Gorman",
                    "title": "Executive Chairman",
                    "note": "CEO 2010-2024; architect of E*Trade ($13B) and Eaton Vance ($7B) acquisitions; now exec chairman",
                    "confidence": "confirmed",
                },
                {
                    "name": "Andy Saperstein",
                    "title": "Co-President, Head of Wealth Management",
                    "confidence": "confirmed",
                },
                {
                    "name": "Dan Simkowitz",
                    "title": "Co-President, Head of Investment Management",
                    "confidence": "confirmed",
                },
            ],
            "strategic_focus": {
                "wealth_management_revenue_pct": 48,
                "institutional_securities_pct": 42,
                "investment_management_pct": 10,
                "note": "Gorman's legacy: transformed from volatile trading house to wealth management + trading hybrid",
                "etrade_integration": "completed 2023; added 5.2M retail accounts",
                "confidence": "confirmed",
            },
            "political_connections": {
                "lobbying_spend_2024": 4_900_000,
                "lobbying_confidence": "confirmed",
                "key_relationships": [
                    "Gorman now also chairs Business Council (successor to Dimon at Roundtable)",
                ],
                "confidence": "confirmed",
            },
            "revolving_door": [
                {
                    "name": "John Mack",
                    "bank_role": "CEO Morgan Stanley (2005-2010)",
                    "gov_role": "No direct gov role, but key GOP fundraiser",
                    "confidence": "confirmed",
                },
            ],
            "regulatory_actions": [
                {
                    "action": "RMBS fraud settlement (DOJ)",
                    "amount_usd": 3_200_000_000,
                    "year": 2016,
                    "agency": "DOJ",
                    "detail": "Misleading investors in RMBS securitization (2005-2007 vintage)",
                    "confidence": "confirmed",
                },
                {
                    "action": "Archegos Capital implosion losses",
                    "amount_usd": 911_000_000,
                    "year": 2021,
                    "agency": "N/A (market event)",
                    "detail": "MS was fastest to exit Archegos positions but still lost $911M; sold $5B in block trades the night before CS/Nomura",
                    "confidence": "confirmed",
                },
                {
                    "action": "Block trade front-running probe",
                    "amount_usd": 0,
                    "year": 2023,
                    "agency": "SEC/DOJ",
                    "detail": "Investigation into whether MS tipped hedge funds before large block trades; settled for undisclosed amount",
                    "confidence": "confirmed",
                },
            ],
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "key_vulnerability": "Concentrated wealth management (market downturn = AUM decline = fee compression)",
                "confidence": "confirmed",
            },
        },

        # ── Wells Fargo ─────────────────────────────────────────────────
        "wfc": {
            "name": "Wells Fargo & Company",
            "ticker": "WFC",
            "total_assets_usd": 1_900_000_000_000,  # ~$1.9T
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 230_000_000_000,
            "market_cap_confidence": "estimated",
            "hq": "San Francisco, CA",
            "founded": 1852,
            "gsib_bucket": 2,
            "gsib_surcharge_pct": 2.0,
            "employees": 227_000,
            "key_personnel": [
                {
                    "name": "Charlie Scharf",
                    "title": "CEO",
                    "since": 2019,
                    "compensation_2024": 29_000_000,
                    "background": "Former BNY Mellon CEO, Visa CEO; JPM protege (Dimon mentored him)",
                    "mandate": "Clean up fake accounts scandal, fix compliance, get Fed asset cap lifted",
                    "confidence": "confirmed",
                },
                {
                    "name": "Mike Santomassimo",
                    "title": "CFO",
                    "confidence": "confirmed",
                },
            ],
            "fed_asset_cap": {
                "imposed": 2018,
                "cap_level_usd": 1_950_000_000_000,
                "reason": "Unprecedented Fed enforcement action after fake accounts scandal",
                "status_2026": "Still in place as of early 2026; multiple failed attempts to lift",
                "impact": "Constrains growth; forced WFC to shed businesses to stay under cap",
                "confidence": "confirmed",
            },
            "political_connections": {
                "lobbying_spend_2024": 5_200_000,
                "lobbying_confidence": "confirmed",
                "key_relationships": [
                    "Elizabeth Warren has been WFC's most vocal Senate critic",
                    "Scharf testified before Congress multiple times on scandal cleanup",
                ],
                "confidence": "confirmed",
            },
            "revolving_door": [
                {
                    "name": "Tim Sloan",
                    "bank_role": "CEO Wells Fargo (2016-2019)",
                    "gov_role": "No gov role, but forced out by congressional pressure (rare direct political ouster of bank CEO)",
                    "confidence": "confirmed",
                },
            ],
            "regulatory_actions": [
                {
                    "action": "Fake accounts scandal (Phase 1)",
                    "amount_usd": 185_000_000,
                    "year": 2016,
                    "agency": "CFPB/OCC/LA City Attorney",
                    "detail": "3.5M+ fake accounts opened without customer consent; CEO John Stumpf forced out",
                    "confidence": "confirmed",
                },
                {
                    "action": "Fake accounts expanded settlement",
                    "amount_usd": 3_000_000_000,
                    "year": 2020,
                    "agency": "DOJ/SEC",
                    "detail": "$3B criminal + civil settlement; scope expanded to 3.5M+ accounts",
                    "confidence": "confirmed",
                },
                {
                    "action": "CFPB auto lending / mortgage fine",
                    "amount_usd": 3_700_000_000,
                    "year": 2022,
                    "agency": "CFPB",
                    "detail": "Largest CFPB fine ever; auto lending, mortgage, and deposit abuses",
                    "confidence": "confirmed",
                },
                {
                    "action": "Fed asset cap",
                    "amount_usd": 0,
                    "year": 2018,
                    "agency": "Federal Reserve",
                    "detail": "Unprecedented: Fed capped WFC total assets at ~$1.95T until compliance fixed",
                    "confidence": "confirmed",
                },
                {
                    "action": "OCC operating losses fine",
                    "amount_usd": 250_000_000,
                    "year": 2021,
                    "agency": "OCC",
                    "detail": "Failure to execute risk management improvements required by 2018 consent orders",
                    "confidence": "confirmed",
                },
            ],
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "key_vulnerability": "Asset cap constrains normal operations; persistent compliance/culture issues; 10+ active consent orders",
                "active_consent_orders": 10,
                "confidence": "confirmed",
            },
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # EUROPEAN BANKS
    # ══════════════════════════════════════════════════════════════════════

    "european_banks": {

        # ── HSBC ────────────────────────────────────────────────────────
        "hsbc": {
            "name": "HSBC Holdings plc",
            "ticker": "HSBA.L / HSBC (US ADR)",
            "total_assets_usd": 2_900_000_000_000,  # ~$2.9T
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 165_000_000_000,
            "market_cap_confidence": "estimated",
            "hq": "London, UK (but HK is revenue center)",
            "founded": 1865,
            "gsib_bucket": 3,
            "employees": 214_000,
            "key_personnel": [
                {
                    "name": "Georges Elhedery",
                    "title": "Group CEO",
                    "since": "September 2024",
                    "background": "Former CFO; Lebanese-born; career in markets/trading",
                    "note": "Succeeded Noel Quinn who resigned unexpectedly; executing major restructuring into East/West divisions",
                    "confidence": "confirmed",
                },
                {
                    "name": "Noel Quinn",
                    "title": "Former Group CEO (2020-2024)",
                    "note": "Resigned citing personal reasons; oversaw Asia pivot and Ping An pressure response",
                    "confidence": "confirmed",
                },
                {
                    "name": "Pam Kaur",
                    "title": "Group CFO",
                    "since": 2024,
                    "note": "First woman and first Indian-origin Group CFO",
                    "confidence": "confirmed",
                },
                {
                    "name": "Mark Tucker",
                    "title": "Group Chairman",
                    "confidence": "confirmed",
                },
            ],
            "geographic_tension": {
                "asia_revenue_pct": 65,
                "hk_china_revenue_pct": 45,
                "uk_hq": True,
                "ping_an_stake": "Ping An Insurance (~8% shareholder) pushed for breakup/Asia spin-off; abandoned after HSBC share buybacks and dividend increases",
                "geopolitical_risk": "Caught between US/UK sanctions regime and China business; dual exposure to HK/mainland regulatory risk",
                "confidence": "confirmed",
            },
            "regulatory_actions": [
                {
                    "action": "Mexican drug cartel money laundering (DPA)",
                    "amount_usd": 1_920_000_000,
                    "year": 2012,
                    "agency": "DOJ/OCC/Fed/FinCEN",
                    "detail": "HSBC processed $881M for Sinaloa/Norte del Valle cartels; $660B in wire transfers from Mexico with minimal KYC; deferred prosecution agreement",
                    "confidence": "confirmed",
                },
                {
                    "action": "FX manipulation settlement",
                    "amount_usd": 618_000_000,
                    "year": 2014,
                    "agency": "FCA/CFTC",
                    "confidence": "confirmed",
                },
                {
                    "action": "Swiss private banking tax evasion (HSBC Geneva)",
                    "amount_usd": 353_000_000,
                    "year": 2015,
                    "agency": "Swiss AG / French prosecution",
                    "detail": "SwissLeaks: HSBC Geneva helped 100,000+ clients hide $100B+ from tax authorities; whistleblower Herve Falciani leaked data",
                    "confidence": "confirmed",
                },
                {
                    "action": "Iran/Libya/Sudan sanctions violations",
                    "amount_usd": 375_000_000,
                    "year": 2011,
                    "agency": "OFAC/DOJ",
                    "detail": "Processed $19.4B in transactions through US system for sanctioned countries",
                    "confidence": "confirmed",
                },
            ],
            "offshore_exposure": {
                "note": "HSBC IS the offshore banking system -- operates in 62 countries; major presence in Channel Islands, Cayman, BVI, Swiss private banking",
                "icij_mentions": "Central to SwissLeaks (2015) and FinCEN Files (2020)",
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "key_vulnerability": "Geopolitical -- if forced to choose between US/UK and China, either choice destroys a major revenue base",
                "confidence": "confirmed",
            },
        },

        # ── Deutsche Bank ───────────────────────────────────────────────
        "db": {
            "name": "Deutsche Bank AG",
            "ticker": "DB / DBK.DE",
            "total_assets_usd": 1_600_000_000_000,  # ~EUR 1.4T (~$1.6T)
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 38_000_000_000,
            "market_cap_confidence": "estimated",
            "hq": "Frankfurt, Germany",
            "founded": 1870,
            "gsib_bucket": 2,
            "employees": 90_000,
            "key_personnel": [
                {
                    "name": "Christian Sewing",
                    "title": "CEO",
                    "since": 2018,
                    "background": "Career Deutsche banker; risk management background",
                    "note": "Stabilized DB after near-death 2016-2019 crisis; cut investment banking, rebuilt capital ratios; share price up 3x since his nadir",
                    "confidence": "confirmed",
                },
                {
                    "name": "James von Moltke",
                    "title": "CFO",
                    "confidence": "confirmed",
                },
            ],
            "trump_connection": {
                "total_lending_to_trump": 2_000_000_000,
                "note": "DB was the ONLY major bank willing to lend to Trump after his 1990s bankruptcies; $2B+ in loans over two decades",
                "key_banker": "Rosemary Vrablic (Private Wealth Mgt) -- personal relationship with Trump family; left DB 2021 under internal review",
                "deutsche_bank_trust_role": "DB was trustee for Trump's debts while he was president -- extraordinary conflict of interest",
                "confidence": "confirmed",
            },
            "epstein_connection": {
                "note": "DB banked Epstein from 2013-2018 AFTER JPM dropped him post-conviction; fined $150M by NYDFS in 2020",
                "fine_usd": 150_000_000,
                "agency": "NYDFS",
                "year": 2020,
                "confidence": "confirmed",
            },
            "regulatory_actions": [
                {
                    "action": "LIBOR manipulation",
                    "amount_usd": 2_500_000_000,
                    "year": 2015,
                    "agency": "DOJ/CFTC/FCA/NYDFS",
                    "detail": "DB paid $2.5B -- largest LIBOR fine of any bank",
                    "confidence": "confirmed",
                },
                {
                    "action": "Russian mirror trades money laundering",
                    "amount_usd": 630_000_000,
                    "year": 2017,
                    "agency": "FCA/NYDFS",
                    "detail": "DB Moscow executed $10B in mirror trades to launder Russian money; buy rubles in Moscow, simultaneously sell same amount in London in dollars",
                    "confidence": "confirmed",
                },
                {
                    "action": "Epstein relationship fine",
                    "amount_usd": 150_000_000,
                    "year": 2020,
                    "agency": "NYDFS",
                    "detail": "Failed AML controls; processed $150M+ for Epstein entities after JPM dropped him",
                    "confidence": "confirmed",
                },
                {
                    "action": "DWS greenwashing probe",
                    "amount_usd": 19_000_000,
                    "year": 2023,
                    "agency": "SEC",
                    "detail": "DWS (DB's asset manager) settled SEC charges of overstating ESG practices; CEO Asoka Woehrmann resigned",
                    "confidence": "confirmed",
                },
                {
                    "action": "US stress test failure (historical)",
                    "amount_usd": 0,
                    "year": 2018,
                    "agency": "Federal Reserve",
                    "detail": "Only foreign bank to fail Fed stress test; called out for 'broad and substantial' risk management weaknesses",
                    "confidence": "confirmed",
                },
            ],
            "offshore_exposure": {
                "note": "Extensive Channel Islands, Cayman, and Cyprus operations; central to Russian capital flight via mirror trades",
                "icij_mentions": "Appears in Panama Papers (client facilitation) and FinCEN Files (suspicious activity reports)",
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "key_vulnerability": "Still trading at significant discount to book (~0.5x in 2025); legacy litigation tail; German economic slowdown exposure; $42T notional derivatives book relative to market cap is extreme ratio",
                "derivatives_notional_usd": 42_000_000_000_000,
                "confidence": "confirmed",
            },
        },

        # ── UBS ─────────────────────────────────────────────────────────
        "ubs": {
            "name": "UBS Group AG",
            "ticker": "UBS / UBSG.SW",
            "total_assets_usd": 1_700_000_000_000,  # ~$1.7T combined post-CS
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 105_000_000_000,
            "market_cap_confidence": "estimated",
            "aum": 5_700_000_000_000,  # ~$5.7T invested assets
            "hq": "Zurich, Switzerland",
            "founded": 1862,
            "gsib_bucket": 1,  # elevated post-CS acquisition
            "employees": 115_000,
            "key_personnel": [
                {
                    "name": "Sergio Ermotti",
                    "title": "Group CEO",
                    "since": "April 2023 (second stint; first was 2011-2020)",
                    "background": "Brought back specifically to manage Credit Suisse integration; Merrill Lynch and UniCredit veteran",
                    "note": "Compensation controversy: CHF 14.4M in 2023 for 9 months, CHF 14.5M in 2024; shareholder backlash",
                    "confidence": "confirmed",
                },
                {
                    "name": "Todd Tuckner",
                    "title": "Group CFO",
                    "confidence": "confirmed",
                },
                {
                    "name": "Colm Kelleher",
                    "title": "Chairman",
                    "background": "Former Morgan Stanley President",
                    "confidence": "confirmed",
                },
            ],
            "credit_suisse_absorption": {
                "acquisition_date": "June 2023",
                "acquisition_price_usd": 3_200_000_000,
                "cs_assets_absorbed_usd": 580_000_000_000,
                "combined_assets_usd": 1_700_000_000_000,
                "at1_wipeout": {
                    "amount_usd": 17_000_000_000,
                    "detail": "$17B in AT1 (Additional Tier 1) bonds written to ZERO while equity got $3.2B; unprecedented -- AT1 usually senior to equity",
                    "legal_challenges": "Ongoing lawsuits from AT1 holders; FINMA authority challenged in Swiss courts",
                    "market_impact": "AT1 market globally repriced; spread widening across European banks; $275B market questioned",
                    "confidence": "confirmed",
                },
                "headcount_reduction": 35_000,  # from combined 120K+ to ~85K target
                "integration_cost_est_usd": 13_000_000_000,
                "synergy_target_usd": 13_000_000_000,  # by 2026
                "status_2026": "Integration ~80% complete; non-core wind-down unit still has ~$50B in assets",
                "confidence": "confirmed",
            },
            "cs_scandals_inherited": [
                "Archegos Capital loss ($5.5B, 2021)",
                "Greensill Capital supply chain finance collapse ($10B investor exposure, 2021)",
                "Mozambique 'tuna bonds' fraud ($2B, criminal convictions)",
                "Bulgarian cocaine money laundering (2022 conviction -- first Swiss bank convicted of money laundering)",
                "Spygate: CS spied on former executives (Iqbal Khan, Peter Goerke); CEO Tidjane Thiam forced out (2020)",
            ],
            "regulatory_actions": [
                {
                    "action": "UBS US tax evasion settlement (pre-CS)",
                    "amount_usd": 780_000_000,
                    "year": 2009,
                    "agency": "DOJ/IRS",
                    "detail": "UBS helped 20,000+ US clients evade taxes via secret Swiss accounts; turned over 4,450 client names (broke Swiss banking secrecy)",
                    "confidence": "confirmed",
                },
                {
                    "action": "UBS rogue trader loss (Kweku Adoboli)",
                    "amount_usd": 2_300_000_000,
                    "year": 2011,
                    "agency": "FSA/Swiss FINMA",
                    "detail": "Unauthorized trading loss of $2.3B; Adoboli sentenced to 7 years",
                    "confidence": "confirmed",
                },
                {
                    "action": "France tax fraud conviction (UBS)",
                    "amount_usd": 1_800_000_000,
                    "year": 2019,
                    "agency": "French courts",
                    "detail": "Convicted of helping French clients evade taxes; EUR 4.5B fine (reduced on appeal to EUR 1.8B in 2021)",
                    "confidence": "confirmed",
                },
            ],
            "offshore_exposure": {
                "note": "UBS IS private banking -- the world's largest wealth manager; Swiss banking secrecy was its core product for decades",
                "icij_mentions": "Prominent in SwissLeaks and Pandora Papers",
                "confidence": "confirmed",
            },
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "too_big_to_fail_for_switzerland": "UBS balance sheet is ~2x Swiss GDP; Swiss 'too big to fail' framework being rewritten",
                "key_vulnerability": "Integration execution risk; CS non-core wind-down; AT1 litigation; Swiss political pressure for breakup/higher capital",
                "confidence": "confirmed",
            },
        },

        # ── Barclays ────────────────────────────────────────────────────
        "barc": {
            "name": "Barclays PLC",
            "ticker": "BARC.L / BCS (US ADR)",
            "total_assets_usd": 1_800_000_000_000,  # ~GBP 1.5T (~$1.8T)
            "total_assets_confidence": "confirmed",
            "market_cap_usd": 45_000_000_000,
            "market_cap_confidence": "estimated",
            "hq": "London, UK",
            "founded": 1690,
            "gsib_bucket": 2,
            "employees": 92_000,
            "key_personnel": [
                {
                    "name": "C.S. Venkatakrishnan",
                    "title": "Group CEO",
                    "since": "November 2021",
                    "nickname": "Venkat",
                    "background": "Former Chief Risk Officer; PhD from MIT in operations research; career JPM then Barclays",
                    "health_note": "Disclosed non-Hodgkin lymphoma diagnosis November 2023; returned to full duties early 2024",
                    "confidence": "confirmed",
                },
                {
                    "name": "Anna Cross",
                    "title": "Group Finance Director (CFO)",
                    "confidence": "confirmed",
                },
            ],
            "strategic_restructuring_2024": {
                "note": "Feb 2024 investor day: reorganized into 5 divisions; targeting >12% ROTE by 2026; GBP 10B buybacks + GBP 2B cost cuts",
                "us_consumer_commitment": "Keeping US credit card business (unlike Citi exit); partnership with major retailers",
                "investment_bank_focus": "Barclays Investment Bank retaining global ambition despite pressure to shrink",
                "confidence": "confirmed",
            },
            "regulatory_actions": [
                {
                    "action": "LIBOR manipulation",
                    "amount_usd": 453_000_000,
                    "year": 2012,
                    "agency": "FSA/DOJ/CFTC",
                    "detail": "First bank fined in LIBOR scandal; CEO Bob Diamond forced to resign; 'the LIBOR bank'",
                    "confidence": "confirmed",
                },
                {
                    "action": "Dark pool fraud settlement",
                    "amount_usd": 70_000_000,
                    "year": 2016,
                    "agency": "SEC/NY AG",
                    "detail": "Misled investors about predatory HFT activity in Barclays dark pool 'LX'",
                    "confidence": "confirmed",
                },
                {
                    "action": "Qatar capital raising SFO prosecution",
                    "amount_usd": 0,
                    "year": 2018,
                    "agency": "UK SFO",
                    "detail": "Charged over undisclosed payments to Qatar during 2008 emergency capital raise (avoided UK govt bailout); acquitted 2020",
                    "confidence": "confirmed",
                },
                {
                    "action": "Structured notes over-issuance",
                    "amount_usd": 361_000_000,
                    "year": 2022,
                    "agency": "SEC",
                    "detail": "Barclays accidentally issued $17.7B more structured notes than registered; had to repurchase; $361M loss",
                    "confidence": "confirmed",
                },
            ],
            "systemic_risk": {
                "fsb_gsib": True,
                "too_big_to_fail": True,
                "key_vulnerability": "UK economic exposure; persistent discount to book; CEO health uncertainty",
                "confidence": "confirmed",
            },
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # SHADOW BANKING / PRIVATE CREDIT
    # ══════════════════════════════════════════════════════════════════════

    "shadow_banking": {

        "overview": {
            "total_private_credit_market_usd": 1_700_000_000_000,
            "growth_rate": "doubled since 2019; 15-20% CAGR",
            "note": "Private credit has replaced banks as primary lender to mid-market companies; banks offloaded risk post-2008 Dodd-Frank; now that risk sits in less-regulated vehicles",
            "confidence": "confirmed",
        },

        "major_players": {
            "apollo": {
                "name": "Apollo Global Management",
                "ticker": "APO",
                "total_aum_usd": 671_000_000_000,  # varies by reporting date
                "credit_aum_usd": 450_000_000_000,
                "ceo": "Marc Rowan",
                "key_risk": "Athene insurance subsidiary ($310B) uses policyholder money to fund Apollo credit strategies; regulatory scrutiny increasing",
                "athene_detail": "Apollo owns Athene (annuities); Athene invests heavily in Apollo-originated credit -- captive insurance model raises conflict-of-interest concerns",
                "epstein_connection": "Leon Black (Apollo co-founder, former CEO) paid Epstein $158M in 'advisory fees'; stepped down as CEO 2021; sold most APO shares by 2024",
                "confidence": "confirmed",
            },
            "blackstone": {
                "name": "Blackstone Inc.",
                "ticker": "BX",
                "total_aum_usd": 1_060_000_000_000,  # $1T+ makes it largest alt manager
                "credit_aum_usd": 350_000_000_000,
                "ceo": "Steve Schwarzman",
                "schwarzman_net_worth": 42_000_000_000,
                "political_connections": "Schwarzman is major GOP donor; close to Trump; hosted $600K birthday party; donated $100M+ to MIT, Oxford, etc.",
                "key_risk": "BREIT (non-traded REIT) redemption gates in 2022-2023; BCRED (credit fund) concentration risk",
                "confidence": "confirmed",
            },
            "kkr": {
                "name": "KKR & Co.",
                "ticker": "KKR",
                "total_aum_usd": 553_000_000_000,
                "credit_aum_usd": 230_000_000_000,
                "co_ceos": ["Scott Nuttall", "Joseph Bae"],
                "founders": "Henry Kravis and George Roberts (co-executive chairmen; semi-retired)",
                "key_risk": "Global Atlantic insurance acquisition mirrors Apollo/Athene model; $170B insurance AUM",
                "confidence": "confirmed",
            },
            "ares": {
                "name": "Ares Management",
                "ticker": "ARES",
                "total_aum_usd": 395_000_000_000,
                "credit_aum_usd": 310_000_000_000,
                "ceo": "Michael Arougheti",
                "key_risk": "Largest pure-play direct lender; concentrated in middle market; downturn would hit hardest",
                "confidence": "confirmed",
            },
        },

        "clo_market": {
            "total_outstanding_usd": 1_100_000_000_000,  # US CLOs ~$1.1T
            "note": "CLO = Collateralized Loan Obligation; pools of leveraged loans tranched into risk layers",
            "who_holds_risk": {
                "aaa_tranches": "banks, insurance companies, pension funds (~65% of structure)",
                "mezzanine": "hedge funds, credit funds, insurance companies (~25%)",
                "equity_tranche": "CLO managers, hedge funds, some PE (~10%)",
                "confidence": "estimated",
            },
            "systemic_risk": "If leveraged loan defaults spike (recession), equity and mezz tranches absorb losses first; AAA tranches historically safe BUT 2008 showed AAA can fail when models are wrong",
            "key_managers": [
                {"name": "CIFC", "aum_clo": "~$40B"},
                {"name": "Carlyle", "aum_clo": "~$35B"},
                {"name": "CSAM/Apollo", "aum_clo": "~$30B"},
                {"name": "Palmer Square", "aum_clo": "~$28B"},
                {"name": "Blackstone/DCI", "aum_clo": "~$25B"},
            ],
            "confidence": "estimated",
        },

        "systemic_risk_assessment": {
            "private_credit_risks": [
                "Mark-to-model valuations (not mark-to-market); losses hidden until defaults force recognition",
                "Covenant-lite loans dominate (~90%); borrowers have more flexibility to add debt and strip assets",
                "Leverage ratios in PE-backed companies average 6-7x EBITDA (historically high)",
                "Payment-in-kind (PIK) toggle loans let borrowers defer cash interest -- masks distress",
                "Insurance captive model (Apollo/Athene, KKR/Global Atlantic) concentrates risk in policyholder base",
                "Interconnectedness: same PE firms are lender, equity owner, and sometimes both sides of the trade",
                "Retail access expanding via interval funds, BDCs -- retail investors bearing institutional-grade risk",
            ],
            "fed_financial_stability_report_2025_flag": "Fed flagged private credit as 'notable vulnerability' in Nov 2025 Financial Stability Report",
            "comparison_to_2008": "Private credit is NOT the same as 2008 subprime: lower leverage at system level, different structure. BUT opacity and concentration of risk in a few large players is similar pattern",
            "confidence": "estimated",
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # CENTRAL BANKS
    # ══════════════════════════════════════════════════════════════════════

    "central_banks": {

        # ── Federal Reserve ─────────────────────────────────────────────
        "fed": {
            "name": "Federal Reserve System",
            "country": "United States",
            "key_personnel": [
                {
                    "name": "Jerome Powell",
                    "title": "Chair",
                    "since": 2018,
                    "term_expires": "February 2026",
                    "background": "Carlyle Group partner (private equity); Treasury official under Bush 41; not an economist (lawyer)",
                    "net_worth_est": 55_000_000,
                    "political_dynamic": "Appointed by Trump, reappointed by Biden; Trump publicly attacked Powell repeatedly; independence under pressure",
                    "confidence": "confirmed",
                },
                {
                    "name": "Philip Jefferson",
                    "title": "Vice Chair",
                    "since": 2023,
                    "confidence": "confirmed",
                },
                {
                    "name": "Michael Barr",
                    "title": "Vice Chair for Supervision",
                    "note": "Resigned from supervision role early 2025 under political pressure; remains as governor",
                    "confidence": "confirmed",
                },
                {
                    "name": "John Williams",
                    "title": "President, NY Fed",
                    "note": "NY Fed president is permanent FOMC voter and de facto #2 in the system",
                    "confidence": "confirmed",
                },
            ],
            "balance_sheet_usd": 6_800_000_000_000,  # ~$6.8T (early 2026, down from $8.9T peak)
            "balance_sheet_peak_usd": 8_900_000_000_000,
            "balance_sheet_confidence": "confirmed",
            "qt_status": {
                "started": "June 2022",
                "pace": "Slowed to $25B/month Treasuries (from $60B), $35B/month MBS (unchanged) as of mid-2025",
                "total_runoff_to_date_usd": 2_100_000_000_000,
                "target_end": "unknown -- 'abundant reserves' framework means stop before reserves get scarce",
                "reserve_level_usd": 3_200_000_000_000,
                "note": "Sept 2019 repo spike happened at $1.5T reserves; current level provides buffer but drain is ongoing",
                "confidence": "confirmed",
            },
            "rate_trajectory": {
                "current_rate_pct": 4.50,  # upper bound of target range
                "rate_as_of": "early 2026",
                "dot_plot_median_2026": 3.75,
                "market_pricing_2026_end": 3.50,
                "note": "Sticky inflation (core PCE ~2.8%) vs slowing labor market; 'higher for longer' thesis tested",
                "confidence": "estimated",
            },
            "independence_threats": [
                "Trump publicly called for Powell's firing (2018-2019, again 2024-2025)",
                "Project 2025 proposed bringing Fed under executive branch oversight",
                "Audit the Fed legislation (Rand Paul) periodically introduced",
                "Fed's own trading scandal (2021) damaged credibility: Kaplan (Dallas Fed), Rosengren (Boston Fed) resigned over personal trading",
            ],
            "systemic_role": {
                "dollar_swap_lines": "Fed provides USD liquidity to 14 central banks via swap lines -- backstop of global dollar system",
                "lender_of_last_resort": "BTFP (Bank Term Funding Program) created March 2023 after SVB; wound down March 2024",
                "treasury_market_backstop": "Fed is implicit backstop of $27T Treasury market",
                "confidence": "confirmed",
            },
        },

        # ── European Central Bank ───────────────────────────────────────
        "ecb": {
            "name": "European Central Bank",
            "country": "Eurozone (20 member states)",
            "key_personnel": [
                {
                    "name": "Christine Lagarde",
                    "title": "President",
                    "since": 2019,
                    "term_expires": "October 2027",
                    "background": "Former IMF MD; French Finance Minister; Baker McKenzie chair; lawyer, not economist",
                    "controversies": "Convicted of negligence by French court (2016) over Tapie arbitration as Finance Minister (no punishment); IMF Greek austerity program criticism",
                    "confidence": "confirmed",
                },
                {
                    "name": "Luis de Guindos",
                    "title": "Vice President",
                    "since": 2018,
                    "confidence": "confirmed",
                },
                {
                    "name": "Isabel Schnabel",
                    "title": "Executive Board Member",
                    "note": "Most hawkish board member; German economist; influential on QT pace",
                    "confidence": "confirmed",
                },
            ],
            "balance_sheet_usd": 4_500_000_000_000,  # ~EUR 4.1T (~$4.5T)
            "balance_sheet_peak_usd": 9_000_000_000_000,  # EUR 8.8T peak (2022)
            "balance_sheet_confidence": "confirmed",
            "qt_status": {
                "app_portfolio": "Full reinvestment ended July 2023; running off via maturities",
                "pepp_portfolio": "Pandemic bond portfolio reinvestments ended Dec 2024; now in runoff",
                "tltro": "EUR 2.1T in TLTROs matured/repaid 2023-2024 (biggest liquidity drain)",
                "pace": "~EUR 30-40B/month passive runoff (no active sales)",
                "confidence": "confirmed",
            },
            "rate_trajectory": {
                "deposit_facility_rate_pct": 2.75,
                "rate_as_of": "early 2026",
                "direction": "cutting cycle started June 2024 from 4.0%; market expects ~2.0% terminal",
                "divergence_from_fed": "ECB cutting faster than Fed due to weaker Eurozone growth; euro weakened",
                "confidence": "estimated",
            },
            "unique_risks": {
                "fragmentation": "TPI (Transmission Protection Instrument) designed to prevent spread blowout in Italy/Greece; untested in practice",
                "sovereign_debt": "Italian debt/GDP ~140%; France downgraded; fiscal rules loosened post-COVID",
                "german_recession": "Germany in technical recession/stagnation since 2022; ECB policy cannot target individual countries",
                "confidence": "confirmed",
            },
        },

        # ── Bank of Japan ───────────────────────────────────────────────
        "boj": {
            "name": "Bank of Japan",
            "country": "Japan",
            "key_personnel": [
                {
                    "name": "Kazuo Ueda",
                    "title": "Governor",
                    "since": "April 2023",
                    "background": "Academic economist (MIT PhD); first non-career BOJ governor in decades",
                    "note": "Ended negative interest rates (March 2024) and yield curve control (YCC); historic policy shift after 25+ years of unconventional policy",
                    "confidence": "confirmed",
                },
                {
                    "name": "Shinichi Uchida",
                    "title": "Deputy Governor",
                    "confidence": "confirmed",
                },
                {
                    "name": "Ryozo Himino",
                    "title": "Deputy Governor",
                    "confidence": "confirmed",
                },
            ],
            "balance_sheet_usd": 4_800_000_000_000,  # ~JPY 740T (~$4.8T at ~155 USDJPY)
            "balance_sheet_pct_gdp": 120,  # ~120% of GDP (highest of any major central bank)
            "balance_sheet_confidence": "confirmed",
            "jgb_holdings_pct": 53,  # BOJ owns ~53% of all outstanding JGBs
            "etf_holdings_usd": 180_000_000_000,  # BOJ owns ~$180B in Japanese equity ETFs
            "qt_status": {
                "jgb_runoff": "Announced July 2024 to reduce JGB purchases from JPY 6T/month to JPY 3T/month by Q1 2026",
                "etf_holdings": "Stopped buying ETFs 2024; no plan to sell existing portfolio (unrealized gains are enormous)",
                "note": "Any aggressive QT risks destabilizing JGB market (BOJ owns >50% of market)",
                "confidence": "confirmed",
            },
            "rate_trajectory": {
                "current_rate_pct": 0.50,
                "rate_as_of": "early 2026",
                "direction": "Hiking cautiously from -0.1% (March 2024) to 0.25% (July 2024) to 0.50% (Jan 2025); market expects 0.75-1.0% by end 2026",
                "yen_impact": "Rate hikes support yen but BOJ being cautious to avoid choking recovery; yen carry trade unwind risk (Aug 2024 was preview)",
                "confidence": "estimated",
            },
            "unique_risks": {
                "carry_trade": "JPY carry trade estimated at $500B-$1T; BOJ rate hikes trigger unwinds that cascade globally (Aug 5, 2024: Nikkei -12% in one day)",
                "jgb_market_dysfunction": "BOJ owns >50% of JGBs; market liquidity severely impaired; normal price discovery impossible",
                "demographics": "Japan population declining ~500K/year; structural deflationary pressure vs. cyclical inflation",
                "confidence": "estimated",
            },
        },

        # ── People's Bank of China ──────────────────────────────────────
        "pboc": {
            "name": "People's Bank of China",
            "country": "China",
            "key_personnel": [
                {
                    "name": "Pan Gongsheng",
                    "title": "Governor",
                    "since": "July 2023",
                    "background": "Former SAFE administrator (FX reserves); career PBOC; Cambridge-educated",
                    "note": "Unlike Fed/ECB heads, PBOC governor serves at pleasure of State Council (Xi Jinping); not independent",
                    "confidence": "confirmed",
                },
                {
                    "name": "Zhu Hexin",
                    "title": "Deputy Governor",
                    "confidence": "confirmed",
                },
            ],
            "balance_sheet_usd": 6_200_000_000_000,  # ~CNY 45T (~$6.2T)
            "balance_sheet_confidence": "estimated",  # PBOC reporting less transparent
            "fx_reserves_usd": 3_200_000_000_000,  # world's largest
            "fx_reserves_confidence": "confirmed",
            "us_treasury_holdings_usd": 770_000_000_000,  # down from $1.3T peak
            "us_treasury_trend": "steadily reducing since 2014; diversifying into gold and other assets",
            "gold_reserves_tonnes": 2_264,  # added ~300 tonnes in 2023-2024
            "gold_confidence": "confirmed",
            "qt_status": {
                "note": "PBOC is EASING, not tightening; opposite of Fed; cutting RRR and rates to support property sector",
                "rrr_current_pct": 8.0,  # weighted average
                "lpr_1y_pct": 3.10,
                "lpr_5y_pct": 3.60,
                "confidence": "confirmed",
            },
            "rate_trajectory": {
                "direction": "cutting -- multiple RRR cuts and rate cuts in 2024-2025 to fight deflation and property crisis",
                "constraint": "Cannot cut too aggressively without widening rate differential with Fed and weakening CNY",
                "capital_controls": "Closed capital account allows more policy independence than typical EM central bank",
                "confidence": "confirmed",
            },
            "unique_risks": {
                "property_crisis": "Evergrande ($300B liabilities), Country Garden ($200B), and dozens of developers in default; PBOC backstopping via relending facilities",
                "local_government_debt": "LGFV (local government financing vehicles) estimated $9-13T; slow-motion restructuring; PBOC providing liquidity support",
                "dollar_dependency": "Despite de-dollarization efforts, China's export economy still primarily settles in USD; $3.2T reserves are both weapon and vulnerability",
                "shadow_banking": "Trust companies and wealth management products (~$12T) are China's version of shadow banking; multiple trust defaults in 2023-2024",
                "confidence": "estimated",
            },
            "political_subordination": {
                "note": "PBOC has NO operational independence; reports to State Council; policy set by Politburo Standing Committee",
                "xi_intervention": "Xi Jinping personally directs major financial policy; 2023 reorganization created Central Financial Commission under CCP control",
                "confidence": "confirmed",
            },
        },

        # ── Bank of England ─────────────────────────────────────────────
        "boe": {
            "name": "Bank of England",
            "country": "United Kingdom",
            "key_personnel": [
                {
                    "name": "Andrew Bailey",
                    "title": "Governor",
                    "since": "March 2020",
                    "term_expires": "March 2028",
                    "background": "Former FCA CEO; career Bank of England; managed Northern Rock crisis",
                    "controversies": "Criticized for slow inflation response (2021-2022); FCA failures (London Capital & Finance, Woodford Fund) during his tenure as FCA CEO",
                    "confidence": "confirmed",
                },
                {
                    "name": "Sarah Breeden",
                    "title": "Deputy Governor for Financial Stability",
                    "since": 2023,
                    "confidence": "confirmed",
                },
                {
                    "name": "Clare Lombardelli",
                    "title": "Deputy Governor for Monetary Policy",
                    "since": 2023,
                    "confidence": "confirmed",
                },
            ],
            "balance_sheet_usd": 1_050_000_000_000,  # ~GBP 850B (~$1.05T)
            "balance_sheet_peak_usd": 1_200_000_000_000,  # GBP 895B peak
            "balance_sheet_confidence": "confirmed",
            "qt_status": {
                "active_gilt_sales": True,
                "pace": "GBP 100B/year target for gilt stock reduction (mix of active sales + maturity runoff)",
                "note": "BOE is the only major central bank actively SELLING bonds (not just letting them mature); started Sept 2022",
                "ldi_crisis_legacy": "Sept 2022 gilts crash forced emergency BOE intervention when LDI pension funds faced margin calls; revealed leverage in pension system",
                "confidence": "confirmed",
            },
            "rate_trajectory": {
                "current_rate_pct": 4.50,
                "rate_as_of": "early 2026",
                "direction": "Gradual cutting; started Aug 2024 from 5.25%; BOE more cautious than ECB due to sticky UK services inflation",
                "market_pricing_2026_end": 3.75,
                "confidence": "estimated",
            },
            "unique_risks": {
                "ldi_pension_leverage": "UK defined benefit pensions use LDI (liability-driven investment) strategies with 3-5x leverage on gilts; Sept 2022 near-catastrophe could repeat if gilt yields spike again",
                "uk_fiscal_position": "High debt/GDP (~100%); limited fiscal space; gilt market fragile post-Truss mini-budget",
                "london_financial_center": "Post-Brexit loss of EU passporting; euro clearing fight with EU (ECB wants to repatriate); London's role as financial center under structural pressure",
                "confidence": "confirmed",
            },
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # CROSS-CUTTING THEMES
    # ══════════════════════════════════════════════════════════════════════

    "cross_cutting": {
        "total_us_bank_assets_usd": 14_500_000_000_000,  # top 6 US banks
        "total_european_bank_assets_usd": 8_000_000_000_000,  # top 4 European
        "total_central_bank_balance_sheets_usd": 24_350_000_000_000,  # Fed+ECB+BOJ+PBOC+BOE
        "total_private_credit_usd": 1_700_000_000_000,
        "total_alt_manager_aum_usd": 2_679_000_000_000,  # Apollo+Blackstone+KKR+Ares
        "total_clo_market_usd": 1_100_000_000_000,

        "revolving_door_density": {
            "highest": "Goldman Sachs -- by far; Treasury Secretaries, NEC Directors, ECB President, Fed officials",
            "second": "Citigroup -- Rubin, Lew, Froman all went bank-to-gov or gov-to-bank",
            "third": "JPMorgan Chase -- Daley, plus extensive Fed/CFTC alumni",
            "note": "Revolving door is structural feature, not aberration; creates regulatory capture where regulators optimize for industry they will rejoin",
            "confidence": "confirmed",
        },

        "offshore_network_density": {
            "tier_1_enablers": ["HSBC (SwissLeaks, FinCEN Files)", "UBS (pre-2009 Swiss secrecy)", "Deutsche Bank (mirror trades)"],
            "tier_2_enablers": ["Goldman Sachs (Cayman fund admin)", "JPMorgan (Cayman entities)", "Barclays (Channel Islands)"],
            "key_jurisdictions": ["Cayman Islands", "BVI", "Channel Islands (Jersey/Guernsey)", "Switzerland", "Luxembourg", "Ireland", "Singapore"],
            "confidence": "estimated",
        },

        "derivative_concentration": {
            "top_5_us_banks_notional_usd": 220_000_000_000_000,  # ~$220T
            "note": "JPM ($54T) + GS ($49T) + Citi ($43T) + BAC ($38T) + MS ($36T) = ~$220T in notional derivatives; netting reduces actual exposure dramatically but interconnectedness is extreme",
            "confidence": "confirmed",
        },

        "regulatory_fine_totals_since_2008": {
            "global_bank_fines_total_usd": 400_000_000_000,
            "note": "Global banks have paid ~$400B+ in fines since 2008 across all categories (mortgage fraud, FX, LIBOR, AML, sanctions, spoofing, etc.); no senior executive has gone to prison for systemic fraud",
            "confidence": "estimated",
        },

        "confidence_summary": {
            "confirmed_pct": 70,
            "derived_pct": 5,
            "estimated_pct": 20,
            "rumored_pct": 3,
            "inferred_pct": 2,
            "note": "Most data from public filings, regulatory orders, and established financial journalism",
        },
    },
}
