"""
GRID Intelligence — Tech Monopoly & Surveillance Capitalism Network Map.

Static intelligence dossier covering the Magnificent Seven, AI arms race
participants, semiconductor chokepoints, and key venture capital influence
nodes. Maps market power, regulatory risk, political connections, data/privacy
controversies, insider trading patterns, and offshore structures.

All data sourced from public filings (10-K, DEF 14A, Form 4, 13F),
DOJ/FTC antitrust filings, SEC enforcement actions, congressional
testimony, EU Commission decisions, ICIJ Offshore Leaks, and credible
investigative journalism.

Confidence labels per GRID convention:
    confirmed  — directly from SEC filings, court rulings, or government databases
    derived    — calculated from confirmed data
    estimated  — credible third-party estimate (Bloomberg, OpenSecrets, etc.)
    rumored    — reported in media but unverified
    inferred   — pattern-detected by GRID analysis

Data vintage: public information through early 2026.
Update frequency: refresh quarterly after proxy season + after major rulings.

Key entry points:
    get_tech_monopoly_network()        — full network dict
    get_company(ticker)                — single company dossier
    get_ai_arms_race()                 — AI companies sub-network
    get_semiconductor_chokepoints()    — chip supply chain map
    get_vc_influence_network()         — venture capital power map
    get_insider_trading_summary()      — aggregated insider sale patterns
    get_regulatory_risk_heatmap()      — regulatory exposure by company
    get_political_connections()        — all political donation/lobbying ties
"""

from __future__ import annotations

import json
from typing import Any


def get_tech_monopoly_network() -> dict[str, Any]:
    """Return the full tech monopoly & surveillance capitalism network."""
    return TECH_MONOPOLY_NETWORK


def get_company(ticker: str) -> dict[str, Any] | None:
    """Return dossier for a single company by ticker."""
    for section in ("mag7", "ai_arms_race", "semiconductors", "venture_capital"):
        sub = TECH_MONOPOLY_NETWORK.get(section, {})
        if ticker in sub:
            return sub[ticker]
    return None


def get_ai_arms_race() -> dict[str, Any]:
    """Return AI arms race sub-network."""
    return TECH_MONOPOLY_NETWORK.get("ai_arms_race", {})


def get_semiconductor_chokepoints() -> dict[str, Any]:
    """Return semiconductor chokepoint map."""
    return TECH_MONOPOLY_NETWORK.get("semiconductors", {})


def get_vc_influence_network() -> dict[str, Any]:
    """Return venture capital influence network."""
    return TECH_MONOPOLY_NETWORK.get("venture_capital", {})


def get_insider_trading_summary() -> list[dict[str, Any]]:
    """Aggregate notable insider trading patterns across all entities."""
    results = []
    for section in ("mag7", "ai_arms_race", "semiconductors", "venture_capital"):
        sub = TECH_MONOPOLY_NETWORK.get(section, {})
        for key, entity in sub.items():
            insider = entity.get("insider_trading", {})
            if insider:
                results.append({
                    "entity": entity.get("name", key),
                    "ticker": entity.get("ticker", key),
                    **insider,
                })
    return results


def get_regulatory_risk_heatmap() -> list[dict[str, Any]]:
    """Return regulatory risk exposure sorted by severity."""
    results = []
    for section in ("mag7", "ai_arms_race", "semiconductors", "venture_capital"):
        sub = TECH_MONOPOLY_NETWORK.get(section, {})
        for key, entity in sub.items():
            reg = entity.get("regulatory_risk", {})
            if reg:
                results.append({
                    "entity": entity.get("name", key),
                    "ticker": entity.get("ticker", key),
                    "overall_risk": reg.get("overall_risk", "unknown"),
                    "antitrust": reg.get("antitrust", {}),
                    "tax_disputes": reg.get("tax_disputes", []),
                    "privacy_enforcement": reg.get("privacy_enforcement", []),
                })
    results.sort(key=lambda r: {"critical": 0, "high": 1, "elevated": 2,
                                 "moderate": 3, "low": 4}.get(r["overall_risk"], 5))
    return results


def get_political_connections() -> list[dict[str, Any]]:
    """Return all political donation and lobbying connections."""
    results = []
    for section in ("mag7", "ai_arms_race", "semiconductors", "venture_capital"):
        sub = TECH_MONOPOLY_NETWORK.get(section, {})
        for key, entity in sub.items():
            pol = entity.get("political_connections", {})
            if pol:
                results.append({
                    "entity": entity.get("name", key),
                    "ticker": entity.get("ticker", key),
                    **pol,
                })
    return results


# ══════════════════════════════════════════════════════════════════════════
# STATIC INTELLIGENCE DATA
# ══════════════════════════════════════════════════════════════════════════

TECH_MONOPOLY_NETWORK: dict[str, Any] = {
    "meta": {
        "report_type": "tech_monopoly_surveillance_capitalism_network",
        "version": "1.0.0",
        "data_vintage": "2026-Q1",
        "refresh_cadence": "quarterly",
        "classification": "OSINT",
        "sources": [
            "SEC EDGAR (10-K, DEF 14A, Form 4, 13F)",
            "DOJ Antitrust Division filings",
            "FTC enforcement actions",
            "EU Commission competition decisions",
            "OpenSecrets.org (lobbying + PAC)",
            "ICIJ Offshore Leaks Database",
            "Bloomberg Billionaires Index",
            "Congressional hearing transcripts",
            "Court filings (Delaware Chancery, DC District, EU CJEU)",
        ],
    },

    # ══════════════════════════════════════════════════════════════════════
    # MAGNIFICENT SEVEN
    # ══════════════════════════════════════════════════════════════════════

    "mag7": {

        # ── APPLE (AAPL) ────────────────────────────────────────────────
        "AAPL": {
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "sector": "Consumer Electronics / Software",
            "market_cap_usd": 3_400_000_000_000,
            "market_cap_confidence": "confirmed",
            "market_cap_note": "Fluctuates; first to $3T Jan 2022, sustained above $3T through 2025",
            "employees": 164_000,
            "hq": "Cupertino, CA",
            "revenue_fy2024_usd": 391_000_000_000,
            "revenue_confidence": "confirmed",
            "cash_on_hand_usd": 162_000_000_000,
            "cash_confidence": "confirmed",
            "source": "10-K FY2024",

            "ceo": {
                "name": "Tim Cook",
                "title": "CEO",
                "appointed": "2011-08-24",
                "total_compensation_2024_usd": 63_200_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2025",
                "background": "Operations specialist; Auburn, Duke MBA; joined Apple 1998 from Compaq; managed supply chain transformation",
                "net_worth_estimated_usd": 2_000_000_000,
                "net_worth_confidence": "estimated",
                "net_worth_source": "Bloomberg Billionaires Index",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "monopoly_gatekeeper",
                "confidence": "confirmed",
                "details": {
                    "app_store": {
                        "description": "iOS App Store is sole distribution channel for 1.5B+ active devices",
                        "commission_rate": "15-30% on digital purchases",
                        "annual_services_revenue_usd": 96_000_000_000,
                        "confidence": "confirmed",
                        "source": "10-K FY2024 services segment",
                    },
                    "china_supply_chain": {
                        "description": "~95% of iPhones assembled in China (Foxconn, Pegatron, Luxshare)",
                        "risk_level": "critical",
                        "diversification_efforts": "India (Tata, Foxconn Chennai) producing ~14% of iPhones by 2025; Vietnam for AirPods/Mac",
                        "china_revenue_pct": 19,
                        "china_revenue_usd": 72_000_000_000,
                        "geopolitical_risk": "Taiwan Strait conflict would halt production; US-China decoupling threatens both supply AND demand",
                        "confidence": "estimated",
                    },
                    "ecosystem_lock_in": {
                        "description": "iMessage, AirDrop, FaceTime, iCloud, Apple Pay, Health create switching costs",
                        "active_devices": 2_200_000_000,
                        "confidence": "estimated",
                    },
                    "search_deal_google": {
                        "description": "Google pays Apple est. $20B/yr to be default search engine on Safari",
                        "annual_payment_est_usd": 20_000_000_000,
                        "confidence": "estimated",
                        "source": "DOJ v. Google trial testimony (Oct 2023)",
                        "at_risk": "DOJ remedy in Google search monopoly case could eliminate this payment",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "high",
                "confidence": "confirmed",
                "antitrust": {
                    "epic_v_apple": {
                        "status": "Supreme Court declined certiorari; injunction requires Apple allow external payment links",
                        "ruling_date": "2023-01-forfeited_to_9th_circuit",
                        "impact": "Apple must allow developers to link to external payment methods; still charges 27% 'reduced' commission",
                        "confidence": "confirmed",
                    },
                    "doj_antitrust_suit_2024": {
                        "filed": "2024-03-21",
                        "allegations": "Monopoly maintenance via: suppressing super apps, blocking cross-platform messaging, limiting cloud streaming, restricting NFC access, degrading non-Apple smartwatch experience",
                        "status": "active_litigation",
                        "potential_remedies": "Structural (unlikely) or behavioral; could force interoperability",
                        "confidence": "confirmed",
                    },
                    "eu_dma": {
                        "designation": "Gatekeeper under Digital Markets Act (Sept 2023)",
                        "compliance_issues": "EU fined Apple EUR 1.84B (Mar 2024) for anti-steering in music streaming; ongoing DMA compliance disputes",
                        "confidence": "confirmed",
                    },
                },
                "tax_disputes": [
                    {
                        "case": "EU State Aid — Ireland",
                        "amount_usd": 15_800_000_000,
                        "status": "ECJ upheld EU Commission order Sept 2024; Apple must repay EUR 13B to Ireland",
                        "confidence": "confirmed",
                    },
                ],
                "privacy_enforcement": [
                    {
                        "issue": "App Tracking Transparency (ATT) — positioned as privacy but also competitive weapon against Meta/Google ad businesses",
                        "confidence": "confirmed",
                        "regulatory_view": "EU investigating whether ATT itself is anti-competitive",
                    },
                ],
            },

            "political_connections": {
                "lobbying_annual_usd": 9_800_000,
                "lobbying_confidence": "confirmed",
                "lobbying_source": "Senate LDA filings 2024",
                "pac_total_2024_cycle_usd": 730_000,
                "pac_confidence": "confirmed",
                "key_issues": ["antitrust", "encryption", "China trade", "immigration (H-1B)"],
                "revolving_door": [
                    {
                        "name": "Lisa Jackson",
                        "apple_role": "VP Environment, Policy, Social Initiatives",
                        "government_role": "EPA Administrator 2009-2013 (Obama)",
                        "confidence": "confirmed",
                    },
                    {
                        "name": "Cynthia Hogan",
                        "apple_role": "Former VP Government Affairs",
                        "government_role": "Biden VP office counsel; Senate Judiciary Committee",
                        "confidence": "confirmed",
                    },
                ],
            },

            "insider_trading": {
                "ceo_sales_pattern": "Tim Cook sells regularly on 10b5-1 plan; sold ~$750M in shares since becoming CEO",
                "notable_sales": [
                    {"date": "2024-11", "amount_usd": 50_000_000, "shares": 220_000, "confidence": "confirmed", "source": "Form 4"},
                ],
                "pattern_assessment": "Systematic disposition; no unusual timing detected",
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_structures": [
                    {
                        "entity": "Apple Operations International (Ireland)",
                        "description": "Irish subsidiary that was 'stateless' for tax purposes 2009-2014; collected $30B+ income taxed at <1%",
                        "status": "Restructured post-2015 Irish tax reform; now subject to Irish tax",
                        "confidence": "confirmed",
                        "source": "US Senate PSI hearings (2013); EU State Aid case",
                    },
                    {
                        "entity": "Apple Sales International (Ireland → Jersey)",
                        "description": "Moved IP holding to Jersey (Channel Islands) after Irish tax changes; Jersey has 0% corporate tax",
                        "status": "Active structure",
                        "confidence": "confirmed",
                        "source": "Paradise Papers (ICIJ 2017)",
                    },
                ],
                "effective_tax_rate_reported": 16.0,
                "effective_tax_rate_confidence": "confirmed",
                "offshore_cash_peak_usd": 252_000_000_000,
                "offshore_cash_note": "Repatriated bulk of offshore cash after 2017 Tax Cuts and Jobs Act (15.5% repatriation rate)",
                "confidence": "confirmed",
            },

            "data_privacy_controversies": [
                {
                    "issue": "CSAM scanning reversal",
                    "description": "Announced on-device CSAM scanning (Aug 2021), reversed after privacy backlash (Dec 2022)",
                    "confidence": "confirmed",
                },
                {
                    "issue": "China data localization",
                    "description": "iCloud data for Chinese users stored on state-owned Guizhou-Cloud Big Data (GCBD) servers; encryption keys held in China",
                    "risk": "Chinese government can compel data access without Apple's US legal team involvement",
                    "confidence": "confirmed",
                    "source": "NYT investigation 2021; Apple transparency reports",
                },
                {
                    "issue": "Siri recordings",
                    "description": "Contractors listened to Siri recordings including medical conversations, drug deals, intimate encounters (2019 disclosure)",
                    "resolution": "Apple suspended grading program, added opt-in",
                    "confidence": "confirmed",
                },
            ],
        },

        # ── MICROSOFT (MSFT) ────────────────────────────────────────────
        "MSFT": {
            "name": "Microsoft Corporation",
            "ticker": "MSFT",
            "sector": "Enterprise Software / Cloud / AI",
            "market_cap_usd": 3_100_000_000_000,
            "market_cap_confidence": "confirmed",
            "employees": 228_000,
            "hq": "Redmond, WA",
            "revenue_fy2024_usd": 245_000_000_000,
            "revenue_confidence": "confirmed",
            "cash_on_hand_usd": 80_000_000_000,
            "cash_confidence": "confirmed",
            "source": "10-K FY2024",

            "ceo": {
                "name": "Satya Nadella",
                "title": "Chairman & CEO",
                "appointed": "2014-02-04",
                "total_compensation_2024_usd": 79_100_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2024",
                "background": "Joined Microsoft 1992; ran Cloud & Enterprise; Manipal engineering, UW-Milwaukee MS, Chicago Booth MBA",
                "net_worth_estimated_usd": 1_100_000_000,
                "net_worth_confidence": "estimated",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "platform_monopoly_multi_segment",
                "confidence": "confirmed",
                "details": {
                    "cloud_azure": {
                        "description": "Azure is #2 cloud (24% market share) behind AWS (31%); fastest growing",
                        "annual_revenue_est_usd": 65_000_000_000,
                        "growth_yoy_pct": 29,
                        "confidence": "estimated",
                    },
                    "office_365": {
                        "description": "Dominant enterprise productivity suite; 400M+ paid seats",
                        "market_share_pct": 87,
                        "confidence": "estimated",
                        "source": "Gartner estimates",
                    },
                    "windows": {
                        "description": "73% desktop OS market share globally",
                        "confidence": "confirmed",
                        "source": "StatCounter",
                    },
                    "linkedin": {
                        "description": "1B+ members; monopoly on professional networking; $16B+ annual revenue",
                        "confidence": "estimated",
                    },
                    "gaming": {
                        "description": "Activision Blizzard acquisition ($68.7B, closed Oct 2023); Xbox Game Pass 34M+ subscribers",
                        "activision_deal_closed": "2023-10-13",
                        "confidence": "confirmed",
                    },
                    "openai_partnership": {
                        "description": "Exclusive cloud provider for OpenAI; $13B+ invested; 49% of OpenAI profit share up to a cap",
                        "total_invested_usd": 13_000_000_000,
                        "structure": "Not equity ownership but 'profit interest' capped at returns; complex licensing deal for API/models",
                        "copilot_integration": "GitHub Copilot (15M+ developers), Microsoft 365 Copilot ($30/user/month), Bing Chat",
                        "confidence": "estimated",
                        "source": "Public statements + Semafor/NYT reporting",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "elevated",
                "confidence": "estimated",
                "antitrust": {
                    "activision_review": {
                        "status": "Closed — deal approved by FTC (dropped challenge), UK CMA (restructured), EU Commission",
                        "confidence": "confirmed",
                    },
                    "eu_teams_bundling": {
                        "status": "EU Commission investigating Teams bundling with Office 365 (complaint by Slack/Salesforce)",
                        "potential_fine": "Up to 10% of global revenue",
                        "confidence": "confirmed",
                    },
                    "ftc_openai_scrutiny": {
                        "description": "FTC investigating whether Microsoft's OpenAI investment constitutes a de facto acquisition",
                        "status": "Preliminary inquiry",
                        "confidence": "confirmed",
                        "source": "FTC Chair Khan statements 2024; Bloomberg reporting",
                    },
                },
                "tax_disputes": [
                    {
                        "case": "IRS audit — transfer pricing (2004-2013)",
                        "amount_usd": 28_900_000_000,
                        "status": "IRS issued NOPA (Sept 2023) claiming Microsoft owes $28.9B in back taxes + penalties for shifting profits to Puerto Rico",
                        "microsoft_position": "Disputes assessment; expects multi-year litigation",
                        "confidence": "confirmed",
                        "source": "Microsoft 10-K FY2024 risk disclosures; IRS NOPA",
                    },
                ],
            },

            "political_connections": {
                "lobbying_annual_usd": 10_200_000,
                "lobbying_confidence": "confirmed",
                "pac_total_2024_cycle_usd": 1_500_000,
                "pac_confidence": "estimated",
                "key_issues": ["AI regulation", "antitrust", "government cloud contracts (JEDI/JWCC)", "immigration"],
                "government_contracts": {
                    "description": "Major US government cloud provider; JWCC (Joint Warfighting Cloud Capability) contract holder",
                    "dod_cloud_revenue_est_usd": 5_000_000_000,
                    "confidence": "estimated",
                },
                "revolving_door": [
                    {
                        "name": "Brad Smith",
                        "microsoft_role": "Vice Chair & President",
                        "political_activity": "Frequent congressional testimony; positioned Microsoft as 'responsible AI' leader; key DOJ/antitrust negotiator",
                        "confidence": "confirmed",
                    },
                ],
            },

            "insider_trading": {
                "ceo_sales_pattern": "Nadella sold ~$285M in MSFT shares (Nov 2021) — largest single sale, pre-announced via 10b5-1",
                "notable_sales": [
                    {"date": "2021-11", "amount_usd": 285_000_000, "shares": 838_584, "confidence": "confirmed", "source": "Form 4",
                     "note": "Sold ~50% of holdings; preceded 2022 tech selloff by 2 months"},
                ],
                "pattern_assessment": "Single large sale raised eyebrows but was pre-planned; generally low insider selling",
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_structures": [
                    {
                        "entity": "Microsoft Ireland Operations Limited",
                        "description": "Irish subsidiary handling EMEA licensing; benefited from 'Double Irish' structure pre-2020",
                        "confidence": "confirmed",
                    },
                    {
                        "entity": "Microsoft Puerto Rico (MSPR)",
                        "description": "IRS alleges Microsoft shifted $39B+ in profits to Puerto Rico operations to exploit Section 936/30A tax benefits",
                        "status": "Subject of $28.9B IRS dispute",
                        "confidence": "confirmed",
                        "source": "IRS NOPA 2023; Microsoft 10-K",
                    },
                    {
                        "entity": "Round Island One (Bermuda → Ireland)",
                        "description": "Bermuda-registered, Irish tax-resident entity held $300B+ in assets; largest company in Ireland by assets",
                        "status": "Restructured in 2020s",
                        "confidence": "confirmed",
                        "source": "Irish corporate filings; Bloomberg reporting",
                    },
                ],
                "effective_tax_rate_reported": 18.0,
                "effective_tax_rate_confidence": "confirmed",
            },

            "data_privacy_controversies": [
                {
                    "issue": "Recall AI feature",
                    "description": "Windows Recall takes screenshots every few seconds for AI search; security researchers found data stored in plaintext SQLite DB accessible to malware",
                    "status": "Delayed launch, security rearchitected; still controversial",
                    "confidence": "confirmed",
                },
                {
                    "issue": "LinkedIn data scraping",
                    "description": "700M+ user profiles scraped and sold on dark web (2021); LinkedIn argued public data is not protected",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Government surveillance cooperation",
                    "description": "Named in Snowden NSA PRISM disclosures (2013); provided email, chat, cloud storage access",
                    "confidence": "confirmed",
                    "source": "Snowden documents / Washington Post",
                },
            ],
        },

        # ── GOOGLE / ALPHABET (GOOGL) ───────────────────────────────────
        "GOOGL": {
            "name": "Alphabet Inc.",
            "ticker": "GOOGL",
            "sector": "Search / Advertising / Cloud / AI",
            "market_cap_usd": 2_200_000_000_000,
            "market_cap_confidence": "confirmed",
            "employees": 182_000,
            "hq": "Mountain View, CA",
            "revenue_fy2024_usd": 350_000_000_000,
            "revenue_confidence": "confirmed",
            "cash_on_hand_usd": 111_000_000_000,
            "cash_confidence": "confirmed",
            "source": "10-K FY2024",

            "ceo": {
                "name": "Sundar Pichai",
                "title": "CEO, Alphabet & Google",
                "appointed": "2015-10-02 (Google); 2019-12-03 (Alphabet)",
                "total_compensation_2024_usd": 226_000_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2025",
                "compensation_note": "Vast majority in stock awards; among highest-paid US CEOs",
                "background": "IIT Kharagpur, Stanford MS, Wharton MBA; joined Google 2004; led Chrome, Android, Apps",
                "net_worth_estimated_usd": 1_500_000_000,
                "net_worth_confidence": "estimated",
                "confidence": "confirmed",
            },

            "founders": {
                "larry_page": {
                    "name": "Larry Page",
                    "title": "Co-Founder; Alphabet Board Member",
                    "net_worth_estimated_usd": 156_000_000_000,
                    "voting_power_pct": 26.1,
                    "voting_power_note": "Class B supervoting shares (10x votes); Page + Brin together control ~51% of votes",
                    "confidence": "estimated",
                },
                "sergey_brin": {
                    "name": "Sergey Brin",
                    "title": "Co-Founder; Alphabet Board Member",
                    "net_worth_estimated_usd": 149_000_000_000,
                    "voting_power_pct": 25.1,
                    "confidence": "estimated",
                },
            },

            "market_power": {
                "assessment": "search_monopoly_confirmed_by_court",
                "confidence": "confirmed",
                "details": {
                    "search_monopoly": {
                        "ruling": "US v. Google — Judge Amit Mehta ruled Google is a monopolist in general search and search text advertising (Aug 5, 2024)",
                        "market_share_pct": 89.2,
                        "search_ad_revenue_usd": 198_000_000_000,
                        "distribution_payments": "Paid $26.3B in 2023 to be default search engine (Apple ~$20B, Samsung, Mozilla, etc.)",
                        "remedies_phase": "Remedy trial scheduled 2025; DOJ proposed forcing Chrome divestiture or search syndication changes",
                        "confidence": "confirmed",
                        "source": "DC District Court ruling; DOJ remedy brief",
                    },
                    "youtube": {
                        "description": "2.5B+ monthly active users; dominant video platform",
                        "annual_ad_revenue_usd": 36_000_000_000,
                        "premium_subscribers": 100_000_000,
                        "confidence": "confirmed",
                    },
                    "android": {
                        "description": "72% global mobile OS market share; 3B+ active devices",
                        "antitrust_history": "EU fined Google EUR 4.34B (2018) for Android bundling; EUR 2.42B (2017) for shopping search",
                        "confidence": "confirmed",
                    },
                    "google_cloud": {
                        "description": "#3 cloud (11% share); growing fast but still trailing AWS/Azure",
                        "annual_revenue_usd": 41_000_000_000,
                        "confidence": "confirmed",
                    },
                    "deepmind": {
                        "description": "Google DeepMind (merged with Brain 2023); Gemini model family; leading AI research",
                        "annual_cost_est_usd": 4_000_000_000,
                        "confidence": "estimated",
                    },
                    "waymo": {
                        "description": "Leading autonomous driving; 100K+ paid rides/week in SF, Phoenix, LA",
                        "cumulative_investment_usd": 8_000_000_000,
                        "confidence": "estimated",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "critical",
                "confidence": "confirmed",
                "risk_note": "Most exposed of any tech company — monopoly CONFIRMED by federal court",
                "antitrust": {
                    "us_v_google_search": {
                        "status": "Monopoly found Aug 2024; remedy phase 2025",
                        "potential_remedies": "Chrome divestiture, end of default search agreements, data portability mandates, AI search competition requirements",
                        "financial_impact": "Loss of Apple default deal alone = ~$20B/yr revenue at risk",
                        "confidence": "confirmed",
                    },
                    "us_v_google_adtech": {
                        "status": "Separate DOJ case filed Jan 2023; trial completed Nov 2024",
                        "allegations": "Monopoly in ad exchange, ad server, and publisher ad network markets",
                        "potential_remedy": "Forced sale of Google Ad Manager / DoubleClick",
                        "confidence": "confirmed",
                    },
                    "eu_fines_total_usd": 8_250_000_000,
                    "eu_fines_note": "EUR 4.34B (Android 2018) + EUR 2.42B (Shopping 2017) + EUR 1.49B (AdSense 2019)",
                    "eu_dma_designation": "Gatekeeper (search, maps, browser, Android, YouTube, ads)",
                    "confidence": "confirmed",
                },
                "tax_disputes": [],
                "privacy_enforcement": [
                    {
                        "issue": "GDPR fines",
                        "amount_eur": 150_000_000,
                        "description": "Multiple CNIL (France) fines for cookie consent violations",
                        "confidence": "confirmed",
                    },
                ],
            },

            "political_connections": {
                "lobbying_annual_usd": 13_400_000,
                "lobbying_confidence": "confirmed",
                "lobbying_note": "Consistently top-3 tech lobbying spender",
                "pac_total_2024_cycle_usd": 2_100_000,
                "pac_confidence": "estimated",
                "key_issues": ["antitrust defense", "AI regulation", "Section 230", "content moderation"],
                "revolving_door": [
                    {
                        "name": "Kent Walker",
                        "google_role": "President, Global Affairs & Chief Legal Officer",
                        "political_activity": "Primary government relations strategist; managed DOJ antitrust defense",
                        "confidence": "confirmed",
                    },
                ],
                "notable": "Google employees were top donors to Obama, Clinton, Biden campaigns; company perceived as having strong Democratic ties",
                "confidence": "estimated",
            },

            "insider_trading": {
                "founder_sales": "Page and Brin have sold tens of billions over two decades via 10b5-1 plans",
                "notable_sales": [
                    {"actor": "Sundar Pichai", "date": "2024-07", "amount_usd": 150_000_000, "confidence": "confirmed", "source": "Form 4"},
                ],
                "pattern_assessment": "Steady long-term disposition by founders; Pichai sells substantial blocks annually",
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_structures": [
                    {
                        "entity": "Google Ireland Holdings (Bermuda → Ireland)",
                        "description": "Pioneered 'Double Irish with a Dutch Sandwich' — routed $23B through Netherlands shell to Bermuda (2017 alone)",
                        "status": "Structure wound down after 2020 Irish BEPS reforms; IP consolidated to US",
                        "confidence": "confirmed",
                        "source": "Dutch Chamber of Commerce filings; EU Commission; ICIJ reporting",
                    },
                ],
                "effective_tax_rate_reported": 13.9,
                "effective_tax_rate_confidence": "confirmed",
                "historical_note": "Effective rate was as low as 2.4% in 2011 via offshore structures",
            },

            "data_privacy_controversies": [
                {
                    "issue": "Incognito mode tracking",
                    "description": "Settled class action for $5B (2024) for tracking users in Chrome Incognito mode",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Location tracking",
                    "description": "Settled with 40 state AGs for $391.5M (2022) for tracking location even when users disabled Location History",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Project Nightingale",
                    "description": "Secretly obtained health records of 50M Americans from Ascension Health without patient consent (2019)",
                    "confidence": "confirmed",
                    "source": "WSJ investigation",
                },
                {
                    "issue": "Google Street View WiFi collection",
                    "description": "Street View cars collected WiFi payload data (emails, passwords) in 30+ countries (2010)",
                    "confidence": "confirmed",
                },
            ],
        },

        # ── AMAZON (AMZN) ───────────────────────────────────────────────
        "AMZN": {
            "name": "Amazon.com, Inc.",
            "ticker": "AMZN",
            "sector": "E-Commerce / Cloud / Logistics / AI",
            "market_cap_usd": 2_200_000_000_000,
            "market_cap_confidence": "confirmed",
            "employees": 1_525_000,
            "employees_note": "Second-largest private employer in US after Walmart",
            "hq": "Seattle, WA (corporate); Arlington, VA (HQ2)",
            "revenue_fy2024_usd": 638_000_000_000,
            "revenue_confidence": "confirmed",
            "source": "10-K FY2024",

            "ceo": {
                "name": "Andy Jassy",
                "title": "President & CEO",
                "appointed": "2021-07-05",
                "total_compensation_2024_usd": 29_200_000,
                "compensation_confidence": "confirmed",
                "compensation_note": "Received $212M stock grant in 2021 vesting over 10 years",
                "background": "Harvard MBA; built AWS from scratch; joined Amazon 1997",
                "confidence": "confirmed",
            },

            "founder": {
                "name": "Jeff Bezos",
                "title": "Executive Chairman",
                "net_worth_estimated_usd": 215_000_000_000,
                "net_worth_confidence": "estimated",
                "ownership_pct": 9.4,
                "recent_sales": {
                    "2024_sales_usd": 13_500_000_000,
                    "2024_shares_sold": 70_000_000,
                    "note": "Sold $13.5B in Feb 2024 after relocating from WA to FL (no state income tax)",
                    "tax_motivation": "WA enacted 7% capital gains tax in 2022; FL has no income tax",
                    "confidence": "confirmed",
                    "source": "Form 4 filings Feb 2024",
                },
            },

            "market_power": {
                "assessment": "platform_monopoly_multi_segment",
                "confidence": "confirmed",
                "details": {
                    "aws": {
                        "description": "#1 cloud provider (31% global share); powers Netflix, Airbnb, US intelligence agencies (CIA C2S)",
                        "annual_revenue_usd": 105_000_000_000,
                        "operating_income_usd": 39_000_000_000,
                        "operating_margin_pct": 37,
                        "confidence": "confirmed",
                        "note": "AWS generates majority of Amazon's operating profit; subsidizes retail",
                    },
                    "ecommerce": {
                        "description": "40% of US e-commerce; Amazon marketplace hosts 60%+ of units sold on the platform (3P sellers)",
                        "ftc_allegation": "Charges sellers 50%+ of revenue in fees (referral, FBA, advertising) — up from 19% in 2014",
                        "confidence": "estimated",
                        "source": "FTC complaint; Marketplace Pulse analysis",
                    },
                    "advertising": {
                        "description": "#3 digital ad platform (after Google, Meta); $53B+ annual ad revenue",
                        "annual_revenue_usd": 53_000_000_000,
                        "confidence": "confirmed",
                    },
                    "logistics": {
                        "description": "Largest delivery fleet in US; 1,000+ fulfillment centers; Amazon Air fleet; last-mile delivery partner (DSP) network",
                        "confidence": "estimated",
                    },
                    "anthropic_investment": {
                        "description": "Up to $4B committed to Anthropic; strategic AI partnership",
                        "total_committed_usd": 4_000_000_000,
                        "confidence": "confirmed",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "high",
                "confidence": "confirmed",
                "antitrust": {
                    "ftc_v_amazon": {
                        "filed": "2023-09-26",
                        "court": "Western District of Washington",
                        "allegations": "Monopoly maintenance via: anti-discounting policy (punishes sellers for lower prices elsewhere), coerced FBA usage, degraded search results to prioritize ad revenue, self-preferencing Amazon brands",
                        "status": "Active litigation; trial expected 2025-2026",
                        "co_plaintiffs": "17 state attorneys general",
                        "confidence": "confirmed",
                    },
                },
                "labor_issues": [
                    {
                        "issue": "Warehouse injury rates",
                        "description": "Amazon warehouse injury rates 2x industry average (6.6 per 100 workers); OSHA citations",
                        "confidence": "confirmed",
                        "source": "Strategic Organizing Center report; OSHA records",
                    },
                    {
                        "issue": "Union busting allegations",
                        "description": "NLRB found Amazon illegally interfered with Bessemer, AL union vote (2022); JFK8 Staten Island voted to unionize (2022) but Amazon contested",
                        "confidence": "confirmed",
                    },
                    {
                        "issue": "Delivery driver classification",
                        "description": "DSP (Delivery Service Partner) model — drivers technically employed by small contractors, not Amazon, limiting liability",
                        "confidence": "confirmed",
                    },
                ],
            },

            "political_connections": {
                "lobbying_annual_usd": 21_400_000,
                "lobbying_confidence": "confirmed",
                "lobbying_note": "Largest lobbying spender in tech",
                "pac_total_2024_cycle_usd": 1_800_000,
                "pac_confidence": "estimated",
                "key_issues": ["antitrust", "labor law", "tax policy", "government cloud (JWCC)", "delivery regulations"],
                "bezos_media_ownership": {
                    "asset": "The Washington Post",
                    "acquired": "2013",
                    "purchase_price_usd": 250_000_000,
                    "political_influence": "Major US newspaper; editorial independence claimed but ownership creates implicit leverage",
                    "confidence": "confirmed",
                },
                "bezos_blue_origin": {
                    "description": "Blue Origin competes for NASA/DoD space contracts; $3.4B HLS lunar lander contract",
                    "confidence": "confirmed",
                },
            },

            "insider_trading": {
                "bezos_sales_total_est_usd": 50_000_000_000,
                "bezos_sales_note": "Bezos has sold ~$50B in Amazon shares since IPO; $13.5B in Feb 2024 alone after FL relocation",
                "pattern_assessment": "Strategic tax-motivated selling; FL move saved est. $600M+ in WA capital gains tax",
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_structures": [
                    {
                        "entity": "Amazon EU SARL (Luxembourg)",
                        "description": "Booked nearly all EU revenue through Luxembourg subsidiary; paid EUR 0 in corporate tax on EUR 44B European sales in 2021",
                        "status": "EU General Court overturned EUR 250M state aid ruling in 2023 (EU lost); Luxembourg structure remains",
                        "confidence": "confirmed",
                        "source": "EU Commission; Luxembourg corporate filings",
                    },
                ],
                "effective_tax_rate_reported": 11.2,
                "us_federal_tax_note": "Amazon paid $0 federal income tax in 2017 and 2018 on $11B+ profits (R&D credits, stock comp deductions, accelerated depreciation)",
                "confidence": "confirmed",
            },

            "data_privacy_controversies": [
                {
                    "issue": "Ring doorbell surveillance network",
                    "description": "Ring shared video with law enforcement without warrants (400+ departments); employees accessed customer videos",
                    "resolution": "FTC settlement $5.8M (2023); ended warrantless police sharing",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Alexa children's recordings",
                    "description": "FTC fined $25M for retaining children's voice recordings and geolocation data in violation of COPPA",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Sidewalk network",
                    "description": "Amazon Sidewalk automatically shares customer WiFi bandwidth with neighbors' devices — opt-out not opt-in",
                    "confidence": "confirmed",
                },
            ],
        },

        # ── NVIDIA (NVDA) ───────────────────────────────────────────────
        "NVDA": {
            "name": "NVIDIA Corporation",
            "ticker": "NVDA",
            "sector": "Semiconductors / AI Compute",
            "market_cap_usd": 3_400_000_000_000,
            "market_cap_confidence": "confirmed",
            "market_cap_note": "Briefly surpassed Apple as most valuable company (2024); extremely volatile",
            "employees": 32_000,
            "hq": "Santa Clara, CA",
            "revenue_fy2025_usd": 130_000_000_000,
            "revenue_confidence": "confirmed",
            "revenue_note": "FY ends Jan; revenue grew 126% YoY driven by data center AI",
            "source": "10-K FY2025",

            "ceo": {
                "name": "Jensen Huang",
                "title": "Co-Founder, President & CEO",
                "appointed": "1993-01-01 (co-founded)",
                "total_compensation_2024_usd": 34_200_000,
                "compensation_confidence": "confirmed",
                "background": "Born Tainan, Taiwan; Oregon State, Stanford MSEE; co-founded NVIDIA at Denny's in 1993",
                "net_worth_estimated_usd": 120_000_000_000,
                "net_worth_confidence": "estimated",
                "ownership_pct": 3.5,
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "ai_chip_monopoly",
                "confidence": "confirmed",
                "details": {
                    "data_center_gpu": {
                        "description": "98% market share in AI training accelerators (H100/H200/B100/B200/GB200)",
                        "market_share_pct": 98,
                        "annual_data_center_revenue_usd": 105_000_000_000,
                        "confidence": "estimated",
                        "source": "JPMorgan, Mercury Research estimates",
                    },
                    "cuda_ecosystem": {
                        "description": "CUDA software ecosystem is the true moat — 4M+ developers; 15+ years of libraries (cuDNN, TensorRT, NCCL)",
                        "switching_cost": "Extremely high; entire AI training stack built on CUDA",
                        "confidence": "confirmed",
                    },
                    "networking": {
                        "description": "Mellanox (acquired $6.9B, 2020) gives InfiniBand monopoly for AI cluster interconnect",
                        "infiniband_share_pct": 80,
                        "confidence": "estimated",
                    },
                    "export_controls": {
                        "description": "US export controls (Oct 2022, Oct 2023, Dec 2024) restrict sales of advanced AI chips to China",
                        "china_revenue_impact": "Lost ~$5B/yr in China revenue; created H800/A800 compliance variants, then those were restricted too",
                        "smuggling_risk": "Reports of H100s being smuggled to China via Singapore, Malaysia intermediaries",
                        "confidence": "confirmed",
                        "source": "BIS rules; Reuters investigations",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "elevated",
                "confidence": "estimated",
                "antitrust": {
                    "doj_inquiry": {
                        "description": "DOJ opened preliminary antitrust inquiry into NVIDIA (2024) — examining whether NVIDIA penalizes customers who don't exclusively use its chips",
                        "status": "Preliminary; no formal complaint",
                        "confidence": "confirmed",
                        "source": "Bloomberg reporting",
                    },
                    "france_dawn_raid": {
                        "description": "French competition authority raided NVIDIA offices (Sept 2023) in cloud computing competition probe",
                        "confidence": "confirmed",
                    },
                },
                "export_control_risk": {
                    "description": "New administrations could tighten or loosen export controls; major revenue uncertainty",
                    "confidence": "estimated",
                },
            },

            "political_connections": {
                "lobbying_annual_usd": 4_200_000,
                "lobbying_confidence": "confirmed",
                "pac_total_2024_cycle_usd": 310_000,
                "pac_confidence": "estimated",
                "key_issues": ["export controls", "CHIPS Act funding", "AI regulation"],
                "chips_act_benefit": {
                    "description": "While NVIDIA doesn't directly receive CHIPS Act fab subsidies (fabless company), its customers (TSMC, Samsung) do",
                    "confidence": "confirmed",
                },
            },

            "insider_trading": {
                "huang_sales_2024_usd": 700_000_000,
                "huang_sales_note": "Jensen Huang sold ~$700M in NVDA shares in 2024 via 10b5-1 plan adopted March 2024",
                "notable_sales": [
                    {"date": "2024-06 through 2024-09", "amount_usd": 700_000_000, "shares": 6_000_000, "confidence": "confirmed", "source": "Form 4 filings"},
                ],
                "pattern_assessment": "Large systematic selling during AI hype peak; 10b5-1 plan provides legal cover but timing was at/near ATH",
                "suspicion_level": "moderate — legal but optically significant",
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_structures": [
                    {
                        "entity": "NVIDIA International (various)",
                        "description": "Significant international operations but less aggressive offshore structuring than peers; primary IP in US",
                        "confidence": "estimated",
                    },
                ],
                "effective_tax_rate_reported": 12.1,
                "effective_tax_rate_note": "Low rate driven by FDII deduction and R&D credits more than offshore structures",
                "confidence": "confirmed",
            },

            "data_privacy_controversies": [
                {
                    "issue": "Training data scraping",
                    "description": "Lawsuit alleges NVIDIA used copyrighted content to train NeMo AI models without authorization",
                    "status": "Pending litigation",
                    "confidence": "confirmed",
                },
            ],
        },

        # ── META PLATFORMS (META) ────────────────────────────────────────
        "META": {
            "name": "Meta Platforms, Inc.",
            "ticker": "META",
            "sector": "Social Media / Advertising / VR-AR / AI",
            "market_cap_usd": 1_600_000_000_000,
            "market_cap_confidence": "confirmed",
            "employees": 72_000,
            "hq": "Menlo Park, CA",
            "revenue_fy2024_usd": 164_000_000_000,
            "revenue_confidence": "confirmed",
            "source": "10-K FY2024",

            "ceo": {
                "name": "Mark Zuckerberg",
                "title": "Chairman, CEO & Controlling Stockholder",
                "founded": "2004-02-04",
                "total_compensation_2024_usd": 32_000_000,
                "compensation_confidence": "confirmed",
                "compensation_note": "Low cash salary ($1); comp is security costs ($27M+) and stock/perks",
                "net_worth_estimated_usd": 210_000_000_000,
                "net_worth_confidence": "estimated",
                "voting_control_pct": 61.2,
                "voting_control_mechanism": "Class B shares (10x voting power); Zuckerberg holds 13.4% economic interest but 61.2% voting control",
                "governance_risk": "No external check on Zuckerberg's authority; board is advisory only in practice",
                "confidence": "confirmed",
                "source": "DEF 14A proxy 2024",
            },

            "market_power": {
                "assessment": "social_media_duopoly_with_advertising_monopoly",
                "confidence": "confirmed",
                "details": {
                    "family_of_apps": {
                        "facebook_mau": 3_070_000_000,
                        "instagram_mau": 2_350_000_000,
                        "whatsapp_mau": 2_780_000_000,
                        "total_daily_active_people": 3_270_000_000,
                        "description": "~40% of humanity uses a Meta product daily",
                        "confidence": "confirmed",
                    },
                    "advertising_dominance": {
                        "description": "Largest social advertising platform; Google+Meta = ~50% of all digital advertising globally",
                        "annual_ad_revenue_usd": 160_000_000_000,
                        "confidence": "confirmed",
                    },
                    "metaverse_reality_labs": {
                        "description": "Reality Labs (VR/AR): Quest headsets, Ray-Ban smart glasses, Horizon Worlds",
                        "cumulative_losses_usd": 58_000_000_000,
                        "annual_losses_2024_usd": 16_000_000_000,
                        "confidence": "confirmed",
                        "note": "Zuckerberg's unilateral $58B+ bet on metaverse — no shareholder vote due to voting control",
                    },
                    "meta_ai": {
                        "description": "Llama open-source model family (Llama 2, Llama 3, Llama 4); strategic counter to closed-source OpenAI/Google",
                        "strategy": "Open-source AI to prevent Google/Microsoft lock-in; commoditize compute layer Meta doesn't control",
                        "ai_capex_2024_usd": 35_000_000_000,
                        "yann_lecun": "Chief AI Scientist; Turing Award winner; vocal advocate for open-source AI",
                        "confidence": "confirmed",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "critical",
                "confidence": "confirmed",
                "antitrust": {
                    "ftc_v_meta": {
                        "filed": "2020-12-09 (refiled 2021)",
                        "allegations": "Monopoly maintenance via acquisitions (Instagram 2012, WhatsApp 2014) and anticompetitive platform policies",
                        "status": "Trial scheduled 2025; FTC seeks structural remedy (forced divestiture of Instagram/WhatsApp)",
                        "potential_impact": "Breakup would destroy ~60% of current market cap",
                        "confidence": "confirmed",
                    },
                },
                "tax_disputes": [
                    {
                        "case": "IRS transfer pricing audit (2008-2013)",
                        "amount_usd": 9_000_000_000,
                        "status": "US Tax Court ruled in IRS's favor (2023) that Meta undervalued IP transferred to Ireland by $8.3B; Meta appealing",
                        "confidence": "confirmed",
                        "source": "US Tax Court decision; Meta 10-K risk disclosures",
                    },
                ],
                "content_regulation": [
                    {
                        "issue": "EU Digital Services Act (DSA)",
                        "description": "Designated Very Large Online Platform; facing investigations for algorithmic amplification of harmful content",
                        "confidence": "confirmed",
                    },
                    {
                        "issue": "Child safety",
                        "description": "41 state AGs + DC sued Meta (Oct 2023) alleging Instagram harms children and uses addictive design features targeting minors",
                        "confidence": "confirmed",
                    },
                ],
            },

            "political_connections": {
                "lobbying_annual_usd": 19_700_000,
                "lobbying_confidence": "confirmed",
                "lobbying_note": "#2 tech lobbying spender after Amazon",
                "pac_total_2024_cycle_usd": 1_200_000,
                "pac_confidence": "estimated",
                "key_issues": ["Section 230", "content moderation mandates", "privacy regulation", "antitrust defense"],
                "election_spending_controversy": {
                    "description": "Zuckerberg and Priscilla Chan donated $400M to Center for Tech and Civic Life (CTCL) for 2020 election administration",
                    "amount_usd": 400_000_000,
                    "controversy": "Critics ('Zuckerbucks') alleged funds disproportionately went to Democratic-leaning urban areas; 28 states subsequently passed laws banning private election funding",
                    "confidence": "confirmed",
                    "source": "CTCL filings; state legislation tracking",
                },
                "board_political_ties": [
                    {
                        "name": "Marc Andreessen",
                        "role": "Board Member (joined 2008)",
                        "political_activity": "Major Republican donor 2024; $4.5M to Trump-aligned PACs; co-authored 'Techno-Optimist Manifesto'; a16z invested in crypto/defense/AI",
                        "dual_role_conflict": "Sits on Meta board while running a16z which invests in Meta competitors",
                        "confidence": "confirmed",
                    },
                    {
                        "name": "Peter Thiel",
                        "role": "Former Board Member (2005-2022)",
                        "political_activity": "Major Trump donor/advisor; Palantir co-founder; funded Hulk Hogan v. Gawker; invested in JD Vance campaign",
                        "confidence": "confirmed",
                    },
                ],
            },

            "insider_trading": {
                "zuckerberg_sales_2024_usd": 1_400_000_000,
                "zuckerberg_sales_note": "Sold ~$1.4B in Meta stock in 2024 via 10b5-1 plan; $42B+ total career sales",
                "chan_zuckerberg_initiative": "Transferred $3B+ in Meta shares to CZI (LLC, not 501c3) — retains voting rights and investment control",
                "pattern_assessment": "Systematic large-scale selling; CZI structure provides tax benefits + maintained voting control",
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_structures": [
                    {
                        "entity": "Facebook Ireland Limited",
                        "description": "Booked international advertising revenue through Ireland; subject of IRS $9B transfer pricing dispute",
                        "status": "Ongoing IRS litigation; structure likely modified post-OECD BEPS Pillar Two",
                        "confidence": "confirmed",
                    },
                    {
                        "entity": "Grand Cayman entities",
                        "description": "Cayman Islands holding entities used in early corporate structure",
                        "confidence": "confirmed",
                        "source": "SEC filings; Paradise Papers",
                    },
                ],
                "effective_tax_rate_reported": 17.6,
                "effective_tax_rate_confidence": "confirmed",
            },

            "data_privacy_controversies": [
                {
                    "issue": "Cambridge Analytica",
                    "description": "87M users' data harvested by political consultancy for 2016 Trump campaign targeting; Facebook allowed API access",
                    "fine_usd": 5_000_000_000,
                    "fine_note": "FTC's largest privacy fine in history (2019)",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Frances Haugen whistleblower",
                    "description": "Internal research showed Instagram harmed teen girls' mental health; Facebook prioritized engagement over safety; algorithm amplified divisive content",
                    "date": "2021-10",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Shadow profiles",
                    "description": "Facebook builds profiles on non-users by tracking them across the web via pixel/SDK and matching contact uploads",
                    "confidence": "confirmed",
                },
                {
                    "issue": "Clearview AI relationship",
                    "description": "Clearview AI scraped billions of Facebook/Instagram photos for facial recognition database used by law enforcement",
                    "confidence": "confirmed",
                },
            ],
        },

        # ── TESLA (TSLA) ────────────────────────────────────────────────
        "TSLA": {
            "name": "Tesla, Inc.",
            "ticker": "TSLA",
            "sector": "EVs / Energy / Autonomous Driving / Robotics",
            "market_cap_usd": 1_100_000_000_000,
            "market_cap_confidence": "confirmed",
            "market_cap_note": "Extremely volatile; ranges $500B-$1.4T; trades at 80-120x forward PE",
            "employees": 140_000,
            "hq": "Austin, TX",
            "revenue_fy2024_usd": 97_000_000_000,
            "revenue_confidence": "confirmed",
            "source": "10-K FY2024",

            "ceo": {
                "name": "Elon Musk",
                "title": "CEO & Technoking",
                "net_worth_estimated_usd": 330_000_000_000,
                "net_worth_confidence": "estimated",
                "net_worth_source": "Bloomberg Billionaires Index — world's richest person",
                "ownership_pct": 12.9,
                "compensation_controversy": {
                    "package": "$56B 2018 stock option plan — largest CEO pay package in history",
                    "delaware_ruling": "Chancellor Kathaleen McCormick voided the package (Jan 2024) as unfair process — board lacked independence, shareholders were misled",
                    "reapproval": "Tesla shareholders re-approved package (Jun 2024); Delaware court rejected re-vote; Tesla reincorporated to Texas",
                    "texas_reincorporation": "Moved incorporation from Delaware to Texas (Jun 2024) to escape Delaware Chancery oversight",
                    "status": "Litigation ongoing; appeal pending",
                    "confidence": "confirmed",
                },
                "simultaneous_ceo_roles": {
                    "description": "Musk simultaneously runs or holds major roles at 6 companies",
                    "companies": ["Tesla (CEO)", "SpaceX (CEO)", "xAI (CEO)", "Neuralink (CEO)", "The Boring Company (Chairman)", "X/Twitter (owner/CTO)"],
                    "governance_risk": "Unprecedented CEO distraction; Tesla board has no leverage to enforce time commitment",
                    "confidence": "confirmed",
                },
                "doge_role": {
                    "description": "Appointed head of Department of Government Efficiency (DOGE) under Trump administration (2025)",
                    "political_donation_2024_usd": 270_000_000,
                    "donation_recipient": "America PAC (pro-Trump super PAC)",
                    "conflicts": "Tesla has $1.5B+ in government contracts (EPA credits, DOD, DOE loans); SpaceX has $15B+ NASA/DOD contracts; Musk now oversees government spending that affects his companies",
                    "confidence": "confirmed",
                    "source": "FEC filings; public announcements",
                },
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "ev_market_leader_declining_share",
                "confidence": "confirmed",
                "details": {
                    "us_ev_market": {
                        "share_pct_2024": 49,
                        "share_pct_2023": 55,
                        "share_pct_2022": 65,
                        "trend": "Declining as legacy automakers and Chinese EVs (BYD) expand",
                        "confidence": "confirmed",
                    },
                    "global_ev_market": {
                        "share_pct": 17,
                        "note": "BYD overtook Tesla in total EV sales (2024) including PHEVs",
                        "confidence": "estimated",
                    },
                    "energy_storage": {
                        "description": "Megapack utility-scale batteries; fastest-growing segment",
                        "revenue_2024_usd": 10_000_000_000,
                        "confidence": "confirmed",
                    },
                    "autonomous_driving": {
                        "description": "FSD (Full Self-Driving) is Level 2 ADAS despite name; Robotaxi (Cybercab) announced Oct 2024",
                        "fsd_revenue_deferred_usd": 3_400_000_000,
                        "safety_record": "NHTSA investigations, multiple recalls; 2 fatal FSD-engaged crashes under investigation",
                        "confidence": "confirmed",
                    },
                    "supercharger_network": {
                        "description": "25,000+ Supercharger stations; NACS became industry standard connector adopted by Ford, GM, Rivian, etc.",
                        "competitive_moat": "Network infrastructure is a durable advantage",
                        "confidence": "confirmed",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "high",
                "confidence": "estimated",
                "risk_note": "Regulatory risk is complex — Musk's DOGE role could REDUCE regulatory scrutiny or create massive conflicts of interest",
                "antitrust": {},
                "sec_enforcement": [
                    {
                        "case": "SEC v. Musk — 'funding secured' tweet (2018)",
                        "outcome": "Settled; Musk/Tesla each paid $20M; Musk removed as chairman; tweets now require pre-approval (widely ignored)",
                        "confidence": "confirmed",
                    },
                    {
                        "case": "SEC investigation — Twitter share disclosure",
                        "description": "Musk filed required 13D late when acquiring 9.2% Twitter stake, saving est. $143M by buying shares at lower prices",
                        "status": "Settled 2025",
                        "confidence": "confirmed",
                    },
                ],
                "safety_regulatory": [
                    {
                        "issue": "NHTSA Autopilot/FSD investigations",
                        "description": "Multiple open investigations into fatal crashes; 2M+ vehicle recall for FSD Beta software update (Dec 2023)",
                        "confidence": "confirmed",
                    },
                ],
                "political_backlash_risk": {
                    "description": "Musk's political polarization (DOGE, Trump alliance, right-wing posting on X) has caused brand damage; Tesla sales declining in Europe and politically progressive US markets",
                    "confidence": "estimated",
                    "source": "EU registration data; Cox Automotive reports",
                },
            },

            "political_connections": {
                "lobbying_annual_usd": 3_100_000,
                "lobbying_confidence": "confirmed",
                "musk_personal_donations": {
                    "2024_cycle_usd": 270_000_000,
                    "recipient": "America PAC (pro-Trump super PAC)",
                    "note": "Largest individual political donor in 2024 election cycle",
                    "confidence": "confirmed",
                    "source": "FEC filings",
                },
                "government_contracts_conflict": {
                    "description": "SpaceX = $15B+ NASA/DOD contracts; Tesla = EPA regulatory credits, DOE Loan (repaid); Starlink = Ukraine/DOD; Boring Co = govt tunnel projects",
                    "doge_conflict": "Musk cutting government programs while his companies depend on government contracts",
                    "confidence": "confirmed",
                },
            },

            "insider_trading": {
                "musk_sales_2022_usd": 23_000_000_000,
                "musk_sales_note": "Sold ~$23B in 2022 to fund Twitter acquisition ($44B buyout); further sales to pay taxes on option exercises",
                "pattern_assessment": "Massive concentrated selling tied to Twitter deal; shares sold at or near cycle highs",
                "confidence": "confirmed",
            },

            "offshore_structures": {
                "known_structures": [
                    {
                        "entity": "Various Dutch and Irish holding entities",
                        "description": "Tesla has international subsidiaries but less aggressive tax structuring than FAANG peers",
                        "confidence": "estimated",
                    },
                ],
                "effective_tax_rate_reported": 10.5,
                "effective_tax_rate_note": "Low rate due to R&D credits, stock comp deductions, and manufacturing deductions",
                "confidence": "confirmed",
            },

            "data_privacy_controversies": [
                {
                    "issue": "In-car camera surveillance",
                    "description": "Tesla vehicles have multiple interior/exterior cameras; employees shared sensitive customer recordings (crying in car, garage nudity, crash videos)",
                    "fine": "Dutch DPA fined Tesla EUR 3.4M for GDPR violations (Aug 2024) — Sentry Mode recorded bystanders without consent",
                    "confidence": "confirmed",
                    "source": "Reuters investigation 2023; Dutch DPA ruling",
                },
                {
                    "issue": "China data localization",
                    "description": "Tesla built dedicated data center in Shanghai to store Chinese customer data locally per CCP requirements",
                    "confidence": "confirmed",
                },
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # AI ARMS RACE
    # ══════════════════════════════════════════════════════════════════════

    "ai_arms_race": {

        # ── OPENAI ──────────────────────────────────────────────────────
        "OPENAI": {
            "name": "OpenAI",
            "ticker": None,
            "type": "private",
            "sector": "Artificial Intelligence",
            "valuation_usd": 300_000_000_000,
            "valuation_confidence": "confirmed",
            "valuation_note": "Based on 2025 tender offer at $300B; was $157B in early 2025, $86B in 2024",
            "employees": 3_500,
            "hq": "San Francisco, CA",
            "annual_revenue_run_rate_usd": 11_600_000_000,
            "revenue_confidence": "confirmed",
            "revenue_note": "ARR as of late 2024; growing ~400% YoY",

            "ceo": {
                "name": "Sam Altman",
                "title": "CEO",
                "background": "Stanford dropout; Y Combinator president (2014-2019); OpenAI co-founded 2015",
                "net_worth_estimated_usd": 2_000_000_000,
                "net_worth_confidence": "estimated",
                "net_worth_note": "Wealth primarily from non-OpenAI investments (Helion, Worldcoin/World, Reddit, Stripe, etc.); claims no equity in OpenAI",
                "board_drama": {
                    "description": "Fired by board Nov 17, 2023; reinstated Nov 22, 2023 after employee revolt (700+ threatened to leave for Microsoft)",
                    "fired_by": ["Helen Toner", "Tasha McCauley", "Adam D'Angelo", "Ilya Sutskever"],
                    "reason_stated": "Board lost confidence in Altman's candor",
                    "outcome": "New board installed (Bret Taylor chair, Larry Summers, Adam D'Angelo retained); Toner, McCauley, Sutskever removed",
                    "confidence": "confirmed",
                },
                "conflicts_of_interest": [
                    "Worldcoin/World (iris-scanning crypto) — Altman is co-founder",
                    "Helion Energy (fusion) — personal investment",
                    "Reddit — personal investment (IPO'd 2024)",
                    "Humane AI Pin — personal investment (failed product)",
                ],
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "ai_model_leader_but_contested",
                "confidence": "estimated",
                "details": {
                    "models": {
                        "gpt4": "GPT-4/4o — leading commercial LLM; ChatGPT 200M+ weekly active users",
                        "o1_o3": "o1/o3 reasoning models — chain-of-thought; strong on math/code",
                        "sora": "Sora — text-to-video model",
                        "whisper": "Whisper — speech recognition (open-sourced)",
                        "dall_e": "DALL-E 3 — image generation",
                        "confidence": "confirmed",
                    },
                    "chatgpt": {
                        "weekly_active_users": 200_000_000,
                        "paying_subscribers_est": 10_000_000,
                        "confidence": "estimated",
                    },
                    "api_platform": {
                        "description": "Leading LLM API provider; powers thousands of applications; 2M+ developers",
                        "confidence": "confirmed",
                    },
                },
            },

            "corporate_structure": {
                "description": "Complex nonprofit-capped_profit hybrid undergoing conversion to for-profit",
                "current_structure": {
                    "parent": "OpenAI Nonprofit (501c3) — technically controls OpenAI Inc.",
                    "subsidiary": "OpenAI Global LLC (capped-profit) — investors get up to 100x return, then profits flow to nonprofit",
                    "cap_structure": "Original investors: 100x cap; later investors: lower caps",
                    "confidence": "confirmed",
                },
                "for_profit_conversion": {
                    "status": "Announced intent to convert to full for-profit PBC (Public Benefit Corporation) by 2026",
                    "nonprofit_compensation": "Nonprofit would receive 'fair value' of its interest; estimated $30-50B",
                    "controversy": "AG of California investigating whether conversion shortchanges the nonprofit mission; Elon Musk sued to block",
                    "elon_musk_lawsuit": {
                        "description": "Musk sued (Feb 2024) alleging OpenAI abandoned nonprofit mission; sought injunction against for-profit conversion",
                        "status": "Active litigation; judge denied preliminary injunction",
                        "musk_motivation": "Founded xAI as competitor; donated $50M to original OpenAI nonprofit; claims breach of founding agreement",
                        "confidence": "confirmed",
                    },
                    "confidence": "confirmed",
                },
            },

            "microsoft_partnership": {
                "total_investment_usd": 13_000_000_000,
                "structure": "Not traditional equity; 'profit interest' — Microsoft gets 49% of profits up to a cap, then share declines",
                "exclusive_cloud": "OpenAI runs exclusively on Azure (worth est. $2B+/yr to Microsoft in compute spend)",
                "model_licensing": "Microsoft has broad license to deploy OpenAI models in its products (Copilot, Bing, Azure OpenAI Service)",
                "tension_points": [
                    "OpenAI building consumer products that compete with Microsoft",
                    "Microsoft investing in competing models (Mistral partnership, internal models)",
                    "Regulatory scrutiny of the deal structure as de facto acquisition",
                ],
                "confidence": "estimated",
            },

            "regulatory_risk": {
                "overall_risk": "high",
                "confidence": "estimated",
                "issues": [
                    "FTC investigating Microsoft-OpenAI deal structure",
                    "California AG scrutinizing nonprofit-to-profit conversion",
                    "EU AI Act — GPT-4 classified as General Purpose AI with systemic risk",
                    "Italy temporarily banned ChatGPT (2023) over GDPR; fined EUR 15M",
                    "Copyright lawsuits: NYT v. OpenAI, Authors Guild v. OpenAI, Getty v. Stability (related)",
                ],
            },

            "political_connections": {
                "altman_dc_activity": "Sam Altman has testified before Congress multiple times; advocates for AI regulation (critics say to create regulatory moat)",
                "lobbying_spend_est_usd": 1_800_000,
                "key_relationships": "Close to both parties; Altman has met with leaders across political spectrum",
                "confidence": "estimated",
            },

            "insider_trading": {
                "note": "Private company — no public market insider trading data",
                "secondary_market": "Employees sold shares in 2024 tender offer at $86B → 2025 tender at $300B; massive paper gains for early employees",
                "confidence": "confirmed",
            },
        },

        # ── ANTHROPIC ───────────────────────────────────────────────────
        "ANTHROPIC": {
            "name": "Anthropic",
            "ticker": None,
            "type": "private",
            "sector": "AI Safety / Large Language Models",
            "valuation_usd": 61_500_000_000,
            "valuation_confidence": "confirmed",
            "valuation_note": "Based on 2024 funding round led by Lightspeed",
            "employees": 1_500,
            "hq": "San Francisco, CA",
            "annual_revenue_run_rate_usd": 2_000_000_000,
            "revenue_confidence": "estimated",

            "ceo": {
                "name": "Dario Amodei",
                "title": "CEO & Co-Founder",
                "background": "PhD Princeton (computational neuroscience); VP Research at OpenAI (2016-2021); left over safety disagreements",
                "sister": "Daniela Amodei — President & Co-Founder",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "safety_positioned_challenger",
                "confidence": "estimated",
                "details": {
                    "models": {
                        "claude_family": "Claude 3.5 Sonnet/Haiku/Opus → Claude 4 family; strong in coding, analysis, safety",
                        "positioning": "Positioned as 'safety-first' alternative to OpenAI; Constitutional AI alignment approach",
                        "confidence": "confirmed",
                    },
                    "enterprise_adoption": {
                        "description": "Growing enterprise API business; partnerships with Notion, DuckDuckGo, Quora, Scale AI",
                        "confidence": "confirmed",
                    },
                },
            },

            "investment_structure": {
                "amazon_investment": {
                    "total_committed_usd": 4_000_000_000,
                    "structure": "Amazon Bedrock integration; Anthropic uses AWS custom chips (Trainium) alongside NVIDIA",
                    "confidence": "confirmed",
                },
                "google_investment": {
                    "total_usd": 2_000_000_000,
                    "structure": "Google Cloud partnership; uses TPUs and GCP",
                    "confidence": "confirmed",
                },
                "other_investors": ["Lightspeed Venture Partners", "Spark Capital", "Menlo Ventures", "Salesforce Ventures"],
                "dual_cloud_strategy": "Unique position using both AWS and GCP — avoids single-vendor dependence",
                "confidence": "confirmed",
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "notes": "Lower risk than OpenAI due to safety positioning and PBC structure; but faces same AI regulation exposure",
                "structure": "Public Benefit Corporation (PBC) — legally bound to consider societal impact, not just shareholder returns",
            },

            "political_connections": {
                "safety_advocacy": "Dario Amodei frequently engages with policymakers on AI safety; published 'Machines of Loving Grace' essay on positive AI futures",
                "confidence": "confirmed",
            },
        },

        # ── GOOGLE DEEPMIND ─────────────────────────────────────────────
        "DEEPMIND": {
            "name": "Google DeepMind",
            "ticker": "GOOGL",
            "type": "subsidiary",
            "sector": "AI Research",
            "parent": "Alphabet Inc.",

            "leader": {
                "name": "Demis Hassabis",
                "title": "CEO, Google DeepMind",
                "background": "Chess prodigy; game designer (Theme Park); PhD UCL cognitive neuroscience; co-founded DeepMind 2010; Nobel Prize Chemistry 2024 (AlphaFold)",
                "net_worth_estimated_usd": 800_000_000,
                "net_worth_confidence": "estimated",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "top_tier_research_lab",
                "confidence": "confirmed",
                "details": {
                    "gemini": {
                        "description": "Gemini model family (Ultra, Pro, Flash, Nano) — Google's flagship AI; competing with GPT-4/Claude",
                        "confidence": "confirmed",
                    },
                    "alphafold": {
                        "description": "AlphaFold solved protein structure prediction; open-sourced; revolutionized biology",
                        "impact": "200M+ protein structures predicted; Hassabis won 2024 Nobel Prize in Chemistry",
                        "confidence": "confirmed",
                    },
                    "other_breakthroughs": ["AlphaGo (2016)", "AlphaZero (2017)", "AlphaCode (2022)", "GraphCast weather (2023)"],
                    "merger": "Google Brain + DeepMind merged into Google DeepMind (Apr 2023) under Hassabis",
                    "annual_cost_est_usd": 4_000_000_000,
                    "confidence": "estimated",
                },
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "note": "Risks are via parent Alphabet; DeepMind's AI safety research provides some regulatory cover",
            },
        },

        # ── xAI ─────────────────────────────────────────────────────────
        "XAI": {
            "name": "xAI Corp.",
            "ticker": None,
            "type": "private",
            "sector": "Artificial Intelligence",
            "valuation_usd": 50_000_000_000,
            "valuation_confidence": "confirmed",
            "valuation_note": "Based on Dec 2024 funding round",
            "employees": 500,
            "employees_confidence": "estimated",
            "hq": "Austin, TX (+ Memphis supercomputer)",

            "ceo": {
                "name": "Elon Musk",
                "title": "CEO & Founder",
                "founded": "2023-03",
                "background": "See TSLA entry for full Musk profile",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "fast_scaling_challenger",
                "confidence": "estimated",
                "details": {
                    "grok": {
                        "description": "Grok model family; integrated into X (Twitter) platform; known for fewer content restrictions",
                        "grok_2": "Released 2024; competitive with GPT-4 on some benchmarks",
                        "x_integration": "Bundled with X Premium+ subscription; trained on X user data",
                        "confidence": "confirmed",
                    },
                    "colossus_cluster": {
                        "description": "Memphis, TN supercomputer cluster — reportedly 100,000 H100 GPUs in single cluster",
                        "gpu_count": 100_000,
                        "investment_est_usd": 4_000_000_000,
                        "build_speed": "Built in 122 days — unprecedented speed",
                        "environmental_concerns": "Temporary generators used; local Memphis environmental/power grid concerns",
                        "confidence": "estimated",
                        "source": "Musk social media posts; Memphis reporting",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "elevated",
                "confidence": "estimated",
                "issues": [
                    "Musk's DOGE role creates massive conflicts — government oversight of AI while building AI company",
                    "X data usage for training may violate GDPR (EU investigating)",
                    "Environmental concerns about Memphis cluster power usage",
                ],
            },

            "political_connections": {
                "musk_influence": "See TSLA entry — Musk's $270M Trump donation and DOGE role directly benefit xAI via reduced AI regulation",
                "confidence": "confirmed",
            },
        },

        # ── META AI (consolidated under META) ───────────────────────────
        "META_AI": {
            "name": "Meta AI (FAIR + GenAI)",
            "ticker": "META",
            "type": "division",
            "sector": "AI Research / Open Source AI",
            "parent": "Meta Platforms",

            "leader": {
                "name": "Yann LeCun",
                "title": "Chief AI Scientist, Meta",
                "background": "Turing Award 2018 (with Bengio, Hinton); pioneer of convolutional neural networks; NYU professor",
                "public_stance": "Vocal critic of AI doomerism; advocates for open-source AI; frequent X debates with AI safety advocates",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "open_source_ai_leader",
                "confidence": "confirmed",
                "details": {
                    "llama": {
                        "description": "Llama model family (Llama 2, 3, 3.1, 4) — most widely adopted open-weight AI models",
                        "strategy": "Open-source to prevent Google/OpenAI from creating AI moat; if AI is commoditized, Meta's data + distribution advantage wins",
                        "downloads": "Hundreds of millions of downloads; adopted by enterprises, startups, governments",
                        "confidence": "confirmed",
                    },
                    "ai_research_spend": {
                        "annual_capex_usd": 35_000_000_000,
                        "note": "Combined AI + metaverse capex; majority now AI-focused",
                        "confidence": "confirmed",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "note": "Open-source AI creates regulatory complexity — who is liable when fine-tuned Llama is misused?",
            },
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # SEMICONDUCTORS — CHOKEPOINTS
    # ══════════════════════════════════════════════════════════════════════

    "semiconductors": {

        # ── TSMC (TSM) ──────────────────────────────────────────────────
        "TSM": {
            "name": "Taiwan Semiconductor Manufacturing Company",
            "ticker": "TSM",
            "sector": "Semiconductor Foundry",
            "market_cap_usd": 900_000_000_000,
            "market_cap_confidence": "confirmed",
            "employees": 76_000,
            "hq": "Hsinchu, Taiwan",
            "revenue_fy2024_usd": 90_000_000_000,
            "revenue_confidence": "confirmed",

            "ceo": {
                "name": "C.C. Wei",
                "title": "Chairman & CEO",
                "appointed": "2024-06 (Chairman); CEO since 2018",
                "background": "30+ year TSMC veteran; PhD electrical engineering; succeeded founder Morris Chang",
                "confidence": "confirmed",
            },

            "founder": {
                "name": "Morris Chang",
                "title": "Founder (retired)",
                "background": "Founded TSMC 1987; invented pure-play foundry model; served as APEC envoy for Taiwan",
                "influence": "Still highly influential in Taiwan tech/political circles",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "most_critical_chokepoint_in_global_tech",
                "confidence": "confirmed",
                "details": {
                    "advanced_node_monopoly": {
                        "description": "TSMC fabricates ~92% of all sub-7nm chips globally",
                        "market_share_advanced_pct": 92,
                        "customers": "Apple (largest), NVIDIA, AMD, Qualcomm, Broadcom, MediaTek — entire AI/mobile ecosystem depends on TSMC",
                        "confidence": "estimated",
                        "source": "TrendForce, Counterpoint Research",
                    },
                    "total_foundry_share_pct": 62,
                    "technology_lead": "N3 (3nm) in production; N2 (2nm) on track for 2025; A16 (1.6nm with backside power) in development",
                    "pricing_power": "Can charge 20-30% premium over Samsung; customers have no alternative for leading-edge",
                    "confidence": "confirmed",
                },
            },

            "geopolitical_risk": {
                "risk_level": "existential",
                "confidence": "confirmed",
                "taiwan_strait": {
                    "description": "China claims Taiwan; military invasion would halt all advanced chip production globally",
                    "silicon_shield_theory": "TSMC's critical role may deter Chinese invasion — destroying TSMC would harm China's own tech sector",
                    "us_response": "CHIPS Act partly motivated by reducing TSMC dependency; US has reportedly planned to destroy TSMC fabs if China invades",
                    "confidence": "estimated",
                    "source": "Bloomberg reporting; US Congressional testimony",
                },
                "arizona_fabs": {
                    "description": "TSMC building 3 fabs in Phoenix, AZ ($65B total investment; $6.6B CHIPS Act subsidy)",
                    "fab_1": "4nm; production started 2025",
                    "fab_2": "3nm/2nm; expected 2028",
                    "fab_3": "2nm; expected 2030",
                    "challenges": "Higher labor costs, cultural friction with US workers, slower construction vs Taiwan",
                    "strategic_value": "Provides US with domestic advanced chip supply in conflict scenario",
                    "confidence": "confirmed",
                },
                "japan_fab": {
                    "description": "JASM (Japan Advanced Semiconductor Manufacturing) — JV with Sony, Denso in Kumamoto",
                    "status": "Fab 1 operational; Fab 2 under construction",
                    "confidence": "confirmed",
                },
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "issues": [
                    "US export controls — TSMC must comply with restrictions on selling advanced chips to China/Huawei",
                    "TSMC allegedly manufactured chips that ended up in Huawei products (Oct 2024 investigation)",
                    "CHIPS Act compliance requirements and restrictions on China fab expansion",
                ],
            },

            "political_connections": {
                "taiwan_government_relationship": "TSMC is effectively a national strategic asset; Taiwanese government holds 6.4% stake via National Development Fund",
                "us_lobbying_usd": 3_600_000,
                "lobbying_confidence": "confirmed",
                "chips_act_subsidy_usd": 6_600_000_000,
                "chips_act_confidence": "confirmed",
            },

            "insider_trading": {
                "note": "Taiwanese insider trading regulations differ from US; limited public data on executive sales",
                "confidence": "estimated",
            },
        },

        # ── SAMSUNG ELECTRONICS ─────────────────────────────────────────
        "005930_KS": {
            "name": "Samsung Electronics Co., Ltd.",
            "ticker": "005930.KS",
            "sector": "Semiconductors / Consumer Electronics",
            "market_cap_usd": 350_000_000_000,
            "market_cap_confidence": "confirmed",
            "employees": 267_000,
            "hq": "Suwon, South Korea",
            "revenue_fy2024_usd": 230_000_000_000,
            "revenue_confidence": "estimated",

            "leader": {
                "name": "Lee Jae-yong (Jay Y. Lee)",
                "title": "Executive Chairman",
                "background": "Grandson of Samsung founder Lee Byung-chul; de facto leader since father Lee Kun-hee's 2014 hospitalization",
                "net_worth_estimated_usd": 11_000_000_000,
                "net_worth_confidence": "estimated",
                "criminal_history": {
                    "bribery_conviction": "Convicted of bribing President Park Geun-hye (2017); sentenced to 5 years; reduced on appeal; presidential pardon (Aug 2022)",
                    "embezzlement": "Convicted of embezzlement and perjury in related cases",
                    "pardon_rationale": "President Yoon pardoned Lee citing national economic interest and semiconductor competition",
                    "confidence": "confirmed",
                },
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "memory_duopoly_foundry_laggard",
                "confidence": "confirmed",
                "details": {
                    "memory": {
                        "dram_share_pct": 42,
                        "nand_share_pct": 34,
                        "description": "Largest memory chip maker (DRAM + NAND); duopoly with SK Hynix in DRAM; oligopoly in NAND (with Micron, WDC/Kioxia)",
                        "hbm": "High Bandwidth Memory — critical for AI (H100/H200); Samsung behind SK Hynix in HBM3E qualification with NVIDIA",
                        "confidence": "confirmed",
                    },
                    "foundry": {
                        "share_pct": 12,
                        "description": "Distant #2 foundry behind TSMC; struggles with yield at 3nm GAA; losing customers",
                        "gap_to_tsmc": "2-3 years behind on yield and reliability; lost Qualcomm flagship orders to TSMC",
                        "confidence": "estimated",
                    },
                    "consumer_electronics": {
                        "description": "#1 smartphone brand globally (20% share); #1 TV brand; appliances",
                        "confidence": "confirmed",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "chaebol_governance": "Samsung group cross-shareholding structure allows Lee family to control ~$400B conglomerate with <5% economic ownership",
                "succession_tax": "Lee family faced $11B inheritance tax bill after Lee Kun-hee's death (2020) — largest in Korean history; selling art, real estate to pay",
                "confidence": "confirmed",
            },

            "political_connections": {
                "chaebol_political_nexus": "Samsung historically has deep ties to Korean government regardless of party; bribery scandal demonstrated extent of influence",
                "us_investments": "Samsung building $17B fab in Taylor, TX (CHIPS Act eligible); $6.4B CHIPS Act subsidy",
                "chips_act_subsidy_usd": 6_400_000_000,
                "confidence": "confirmed",
            },
        },

        # ── INTEL (INTC) ────────────────────────────────────────────────
        "INTC": {
            "name": "Intel Corporation",
            "ticker": "INTC",
            "sector": "Semiconductors / Foundry",
            "market_cap_usd": 90_000_000_000,
            "market_cap_confidence": "confirmed",
            "market_cap_note": "Fallen from $300B+ peak; stock down ~60% from 2021 highs",
            "employees": 110_000,
            "employees_note": "Down from 131K after 15,000+ layoffs (Aug 2024)",
            "hq": "Santa Clara, CA",
            "revenue_fy2024_usd": 54_000_000_000,
            "revenue_confidence": "confirmed",

            "ceo": {
                "name": "Pat Gelsinger",
                "title": "Former CEO (resigned Dec 1, 2024)",
                "tenure": "2021-02-15 to 2024-12-01",
                "departure": {
                    "description": "Forced out by board after failing to execute foundry turnaround; board lost confidence in IDM 2.0 strategy timeline",
                    "interim_ceos": "David Zinsner (CFO) and Michelle Johnston Holthaus as interim co-CEOs",
                    "confidence": "confirmed",
                },
                "background": "Intel's first CTO; left for VMware CEO role 2009; returned as CEO 2021 with IDM 2.0 foundry vision",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "declining_incumbent",
                "confidence": "confirmed",
                "details": {
                    "x86_server": {
                        "description": "Still dominant in x86 server CPUs but losing share to AMD and ARM-based chips (AWS Graviton, Ampere, NVIDIA Grace)",
                        "server_share_pct": 72,
                        "trend": "Down from 98%+ in 2018",
                        "confidence": "estimated",
                    },
                    "foundry_services": {
                        "description": "Intel Foundry Services (IFS) — ambitious plan to become external foundry; Intel 18A (1.8nm) process",
                        "status": "Losing billions annually ($7B operating loss in foundry in 2024); very few external customers signed",
                        "annual_losses_usd": 7_000_000_000,
                        "confidence": "confirmed",
                        "source": "Intel 10-K; earnings calls",
                    },
                    "pc_market": {
                        "description": "Still dominant in PC CPUs but AMD gained significant share; Apple Silicon replaced Intel in Macs entirely",
                        "confidence": "confirmed",
                    },
                    "ai_accelerators": {
                        "description": "Gaudi AI accelerator line has <1% market share; failed to compete with NVIDIA",
                        "confidence": "estimated",
                    },
                },
            },

            "regulatory_risk": {
                "overall_risk": "low",
                "confidence": "estimated",
                "risk_note": "Intel is more likely to receive government support than face regulatory action — seen as strategic national asset",
                "chips_act": {
                    "subsidy_usd": 8_500_000_000,
                    "loans_usd": 11_000_000_000,
                    "description": "Largest CHIPS Act recipient; $8.5B grant + $11B in loans for Ohio, Arizona, New Mexico, Oregon fabs",
                    "political_narrative": "Intel is the only US-owned company with leading-edge fab capability — national security argument",
                    "confidence": "confirmed",
                },
            },

            "political_connections": {
                "lobbying_annual_usd": 5_500_000,
                "lobbying_confidence": "confirmed",
                "chips_act_advocacy": "Intel was primary corporate advocate for CHIPS Act; Gelsinger personally lobbied Congress extensively",
                "national_security_framing": "Positioned as 'only American company that can manufacture advanced chips on US soil'",
                "confidence": "confirmed",
            },

            "insider_trading": {
                "gelsinger_sales": "Gelsinger sold shares periodically via 10b5-1 during tenure while stock declined",
                "pattern_assessment": "CEO selling while executing turnaround raised morale concerns internally",
                "confidence": "estimated",
            },
        },

        # ── ASML (ASML) ─────────────────────────────────────────────────
        "ASML": {
            "name": "ASML Holding N.V.",
            "ticker": "ASML",
            "sector": "Semiconductor Equipment",
            "market_cap_usd": 350_000_000_000,
            "market_cap_confidence": "confirmed",
            "employees": 44_000,
            "hq": "Veldhoven, Netherlands",
            "revenue_fy2024_usd": 30_000_000_000,
            "revenue_confidence": "confirmed",

            "ceo": {
                "name": "Christophe Fouquet",
                "title": "President & CEO",
                "appointed": "2024-04-24",
                "predecessor": "Peter Wennink (CEO 2013-2024)",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "absolute_monopoly_euv",
                "confidence": "confirmed",
                "details": {
                    "euv_lithography": {
                        "description": "ASML is the SOLE manufacturer of EUV (Extreme Ultraviolet) lithography machines — required for all chips below 7nm",
                        "market_share_pct": 100,
                        "machine_cost_usd": 380_000_000,
                        "machine_cost_note": "High-NA EUV (next-gen) costs ~$380M per machine; standard EUV ~$200M",
                        "customers": "TSMC, Samsung, Intel — all dependent on ASML",
                        "annual_production": "~50 EUV systems per year; demand exceeds supply",
                        "confidence": "confirmed",
                    },
                    "duv_lithography": {
                        "description": "Also dominant in DUV (Deep Ultraviolet) — older but still essential; ~62% market share",
                        "competitors": "Nikon, Canon (distant #2, #3 in DUV only)",
                        "confidence": "confirmed",
                    },
                    "why_monopoly_persists": {
                        "description": "EUV machine contains 100,000+ components from 5,000+ suppliers; 20+ years of R&D; $10B+ cumulative investment; no competitor has attempted to replicate",
                        "key_suppliers": ["Carl Zeiss (optics — exclusive supplier)", "TRUMPF (laser source)", "Cymer (light source, ASML-owned)"],
                        "confidence": "confirmed",
                    },
                },
            },

            "geopolitical_significance": {
                "china_export_ban": {
                    "description": "Dutch government (under US pressure) banned export of EUV machines to China (2023); extended to advanced DUV (2024)",
                    "impact_on_china": "China cannot manufacture advanced chips (<7nm) without EUV; forced to develop SMIC workarounds at massive yield penalties",
                    "impact_on_asml": "Lost ~$2B/yr in China revenue; China was 39% of DUV sales",
                    "us_pressure": "Biden administration pressured Netherlands and Japan to restrict chip equipment exports to China",
                    "confidence": "confirmed",
                },
                "strategic_importance": "ASML is arguably the single most strategically important company in the global technology supply chain",
                "confidence": "confirmed",
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "notes": "Main risk is GEOPOLITICAL not regulatory — government export controls directly limit ASML's addressable market",
                "us_china_decoupling": "ASML caught between US pressure to restrict China and China's demand (formerly their fastest-growing market)",
            },

            "political_connections": {
                "dutch_government": "Dutch government has strategic interest in ASML; export controls required Dutch parliamentary approval",
                "us_relationship": "Deep coordination with US Commerce Dept/BIS on export controls",
                "confidence": "confirmed",
            },

            "insider_trading": {
                "note": "Dutch insider trading regulations apply; limited notable patterns",
                "confidence": "estimated",
            },

            "offshore_structures": {
                "note": "Headquartered in Netherlands; standard Dutch holding structure; no notable offshore controversies",
                "confidence": "estimated",
            },
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # VENTURE CAPITAL INFLUENCE NETWORK
    # ══════════════════════════════════════════════════════════════════════

    "venture_capital": {

        # ── ANDREESSEN HOROWITZ (a16z) ──────────────────────────────────
        "A16Z": {
            "name": "Andreessen Horowitz (a16z)",
            "ticker": None,
            "type": "venture_capital",
            "sector": "Technology Investing",
            "aum_usd": 42_000_000_000,
            "aum_confidence": "estimated",
            "hq": "Menlo Park, CA",

            "founders": {
                "marc_andreessen": {
                    "name": "Marc Andreessen",
                    "title": "Co-Founder & General Partner",
                    "net_worth_estimated_usd": 2_100_000_000,
                    "net_worth_confidence": "estimated",
                    "background": "Co-creator of Mosaic/Netscape; 'Software is eating the world' (2011)",
                    "board_seats": ["Meta Platforms (since 2008)"],
                    "political_activity": {
                        "2024_donations_usd": 4_500_000,
                        "recipients": "Trump-aligned PACs, Fairshake crypto super PAC",
                        "techno_optimist_manifesto": "Published Oct 2023 — anti-regulation, pro-acceleration ideology",
                        "doge_connection": "Close to Musk; advocates for government tech reform",
                        "confidence": "confirmed",
                    },
                    "conflicts_of_interest": [
                        "Meta board member while a16z invests in Meta competitors",
                        "Crypto fund ($4.5B) while lobbying against SEC crypto regulation",
                        "Defense tech investments (Anduril, Palantir adjacency) while advocating for less government",
                    ],
                    "confidence": "confirmed",
                },
                "ben_horowitz": {
                    "name": "Ben Horowitz",
                    "title": "Co-Founder & General Partner",
                    "political_activity": {
                        "2024_donations": "Major Trump donor; co-hosted Trump fundraiser in SF",
                        "confidence": "confirmed",
                    },
                },
            },

            "market_power": {
                "assessment": "top_tier_vc_kingmaker",
                "confidence": "confirmed",
                "details": {
                    "portfolio_influence": {
                        "description": "a16z portfolio companies worth $500B+; firm's cultural influence in Silicon Valley shapes which companies get funded and which ideas get amplified",
                        "key_investments": ["Coinbase", "Instacart", "Airbnb (early)", "GitHub (early)", "Databricks", "Anduril", "Mistral"],
                        "confidence": "estimated",
                    },
                    "crypto_dominance": {
                        "description": "$7.6B deployed in crypto/web3; largest crypto VC; Coinbase early investor",
                        "crypto_fund_usd": 7_600_000_000,
                        "confidence": "confirmed",
                    },
                    "defense_tech": {
                        "description": "Major investor in defense tech (Anduril, Shield AI, others); promotes 'American Dynamism' thesis",
                        "confidence": "confirmed",
                    },
                    "media_operation": {
                        "description": "a16z operates like a media company — podcasts, blogs, newsletters shape tech narrative; hired Ben Thompson (Stratechery), Sriram Krishnan (Trump advisor)",
                        "confidence": "confirmed",
                    },
                },
            },

            "political_connections": {
                "fairshake_pac": {
                    "description": "a16z co-funded Fairshake, largest crypto super PAC ($200M+); spent heavily in 2024 congressional races to elect pro-crypto candidates",
                    "total_raised_usd": 200_000_000,
                    "impact": "Defeated anti-crypto incumbents in multiple primaries",
                    "confidence": "confirmed",
                },
                "trump_relationship": {
                    "description": "Andreessen and Horowitz publicly endorsed Trump (Jul 2024); Sriram Krishnan (a16z) became Trump's AI advisor",
                    "confidence": "confirmed",
                },
                "regulatory_stance": "Aggressively anti-SEC (Gary Gensler era); advocates for crypto-friendly regulation; opposes AI regulation",
                "confidence": "confirmed",
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "issues": [
                    "SEC scrutiny of VC-to-crypto pipeline and token investments",
                    "Registered as RIA (Registered Investment Adviser) in 2023 — unusual for VC, gives ability to invest in public tokens",
                ],
            },
        },

        # ── SEQUOIA CAPITAL ─────────────────────────────────────────────
        "SEQUOIA": {
            "name": "Sequoia Capital",
            "ticker": None,
            "type": "venture_capital",
            "sector": "Technology Investing",
            "aum_usd": 85_000_000_000,
            "aum_confidence": "estimated",
            "hq": "Menlo Park, CA",

            "market_power": {
                "assessment": "most_successful_vc_firm_in_history",
                "confidence": "confirmed",
                "details": {
                    "portfolio": {
                        "description": "Backed Apple, Google, Oracle, Cisco, Yahoo, YouTube, Instagram, WhatsApp, Stripe, DoorDash, Snowflake",
                        "confidence": "confirmed",
                    },
                    "split": {
                        "description": "Split into three independent entities (Jun 2024): Sequoia Capital (US/Europe), HongShan (China), Peak XV Partners (India/SEA)",
                        "reason": "US-China geopolitical tension made single global fund untenable; LP pressure over China investments",
                        "confidence": "confirmed",
                    },
                    "ai_investments": {
                        "description": "Major AI investor — early in Hugging Face, Scale AI; backed multiple AI infrastructure companies",
                        "confidence": "confirmed",
                    },
                    "ftx_loss": {
                        "description": "Invested $214M in FTX; wrote down to $0 after FTX collapse (Nov 2022)",
                        "amount_lost_usd": 214_000_000,
                        "confidence": "confirmed",
                    },
                },
            },

            "key_figures": {
                "roelof_botha": {
                    "name": "Roelof Botha",
                    "title": "Managing Partner (US/Europe)",
                    "background": "Former PayPal CFO; South African; Stanford MBA",
                    "confidence": "confirmed",
                },
                "neil_shen": {
                    "name": "Neil Shen (Shen Nanpeng)",
                    "title": "Founding Managing Partner, HongShan (formerly Sequoia China)",
                    "background": "Founded Ctrip and Home Inns; most powerful VC in China",
                    "net_worth_estimated_usd": 4_500_000_000,
                    "political_connections": "CPPCC member (Chinese People's Political Consultative Conference) — advisory body to CCP",
                    "confidence": "confirmed",
                },
            },

            "political_connections": {
                "china_split_significance": "Sequoia's forced China split is emblematic of US-China tech decoupling; HongShan continues to invest in Chinese AI, defense-adjacent tech",
                "us_lobbying": "Lower profile than a16z politically; Sequoia partners have donated across both parties",
                "confidence": "estimated",
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "issues": [
                    "CFIUS (Committee on Foreign Investment) scrutiny of any remaining cross-border portfolio overlap",
                    "LP pressure over China exposure post-split",
                    "FTX loss prompted LP demand for better due diligence governance",
                ],
            },
        },

        # ── SOFTBANK GROUP ──────────────────────────────────────────────
        "9984_T": {
            "name": "SoftBank Group Corp.",
            "ticker": "9984.T",
            "type": "investment_holding",
            "sector": "Technology Investing / Telecom",
            "market_cap_usd": 110_000_000_000,
            "market_cap_confidence": "confirmed",
            "hq": "Tokyo, Japan",
            "nav_usd": 180_000_000_000,
            "nav_confidence": "estimated",
            "nav_note": "Persistent NAV discount (~40%); market does not fully value SoftBank's holdings",

            "ceo": {
                "name": "Masayoshi Son",
                "title": "Chairman & CEO; Founder",
                "net_worth_estimated_usd": 25_000_000_000,
                "net_worth_confidence": "estimated",
                "background": "Born in Japan to ethnic Korean family; UC Berkeley; founded SoftBank 1981; early $20M Yahoo Japan investment returned $100B+",
                "investment_style": "Enormous concentrated bets; 'vision' investing; willing to lose billions on conviction",
                "confidence": "confirmed",
            },

            "market_power": {
                "assessment": "largest_tech_investor_globally",
                "confidence": "confirmed",
                "details": {
                    "vision_fund_1": {
                        "size_usd": 100_000_000_000,
                        "vintage": "2017",
                        "saudi_contribution_usd": 45_000_000_000,
                        "abu_dhabi_contribution_usd": 15_000_000_000,
                        "son_contribution_usd": 28_000_000_000,
                        "performance": "Rocky — WeWork, Wirecard, Greensill losses; but offset by Coupang, DoorDash gains; net positive but below target",
                        "key_failures": ["WeWork ($4.7B write-down)", "Wirecard (fraud)", "Greensill (fraud)", "Katerra (bankruptcy)"],
                        "confidence": "confirmed",
                    },
                    "vision_fund_2": {
                        "size_usd": 56_000_000_000,
                        "vintage": "2019-2022",
                        "note": "Funded entirely by SoftBank Group (no external LPs after VF1 controversies)",
                        "performance": "Significant losses in 2022 tech downturn; recovered partially",
                        "confidence": "confirmed",
                    },
                    "arm_holdings": {
                        "description": "SoftBank owns ~90% of ARM Holdings (re-IPO'd Sept 2023)",
                        "arm_market_cap_usd": 150_000_000_000,
                        "softbank_stake_value_usd": 135_000_000_000,
                        "arm_significance": "ARM designs power virtually all smartphones (99%+) and growing presence in data center/AI",
                        "confidence": "confirmed",
                    },
                    "ai_pivot": {
                        "description": "Son declared AI is SoftBank's singular focus; investing in AI infrastructure, startups, and building Arm-based AI chips",
                        "planned_ai_investment_usd": 100_000_000_000,
                        "project_stargate": "SoftBank co-leading Project Stargate ($500B AI infrastructure initiative with OpenAI, Oracle, others)",
                        "confidence": "estimated",
                    },
                },
            },

            "political_connections": {
                "trump_relationship": {
                    "description": "Son met with Trump post-2016 election, pledged $50B US investment; maintained relationship through both terms",
                    "stargate_announcement": "Co-announced Project Stargate at White House with Trump (Jan 2025)",
                    "confidence": "confirmed",
                },
                "saudi_relationship": {
                    "description": "Mohammed bin Salman (MBS) was largest VF1 LP; PIF committed $45B; relationship survived Khashoggi controversy",
                    "confidence": "confirmed",
                },
                "japan_government": "Close relationship with Japanese government; SoftBank mobile is #3 carrier; Son is Japan's richest person",
                "confidence": "confirmed",
            },

            "regulatory_risk": {
                "overall_risk": "moderate",
                "confidence": "estimated",
                "issues": [
                    "CFIUS scrutiny of ARM sale attempts (NVIDIA acquisition blocked 2022)",
                    "Antitrust concerns around ARM's market position if SoftBank leverages ownership",
                    "Debt-heavy structure — SoftBank Group net debt ~$50B; relies on asset values remaining high",
                    "Sprint merger with T-Mobile completed but regulatory conditions still monitored",
                ],
            },

            "insider_trading": {
                "son_personal_trading": "Son has used personal margin loans against SoftBank shares; nearly margin-called during dot-com bust",
                "arm_lockup": "SoftBank restricted from selling ARM shares for 180 days post-IPO; began measured sales",
                "confidence": "estimated",
            },
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # CROSS-CUTTING ANALYSIS
    # ══════════════════════════════════════════════════════════════════════

    "cross_cutting": {
        "surveillance_capitalism_data_flows": {
            "description": "How user data flows between these entities and becomes revenue",
            "flows": [
                {
                    "from": "Users (3B+ people)",
                    "to": "Google/Meta",
                    "mechanism": "Search queries, social posts, location, browsing history, app usage",
                    "monetization": "Targeted advertising ($360B+ combined annual ad revenue)",
                    "confidence": "confirmed",
                },
                {
                    "from": "Google/Meta",
                    "to": "Data brokers / Advertisers",
                    "mechanism": "Ad targeting APIs, audience segments, lookalike audiences",
                    "privacy_risk": "Individual-level targeting possible without 'selling' data per se",
                    "confidence": "confirmed",
                },
                {
                    "from": "Apple",
                    "to": "Users",
                    "mechanism": "App Tracking Transparency (ATT) — privacy as product AND competitive weapon against Meta/Google",
                    "confidence": "confirmed",
                },
                {
                    "from": "Amazon",
                    "to": "Amazon Advertising",
                    "mechanism": "Purchase history, browsing, Alexa, Ring, Whole Foods — most complete PURCHASE intent dataset",
                    "confidence": "confirmed",
                },
                {
                    "from": "Microsoft",
                    "to": "Enterprise + Government",
                    "mechanism": "LinkedIn professional data, Office 365 telemetry, Windows telemetry, GitHub code",
                    "confidence": "confirmed",
                },
            ],
        },

        "ai_compute_dependency_chain": {
            "description": "The AI supply chain has multiple single points of failure",
            "chain": [
                {"layer": "Equipment", "bottleneck": "ASML (100% EUV monopoly)", "risk": "Dutch export controls"},
                {"layer": "Fabrication", "bottleneck": "TSMC (92% advanced nodes)", "risk": "Taiwan Strait conflict"},
                {"layer": "GPU Design", "bottleneck": "NVIDIA (98% AI training)", "risk": "Export controls, antitrust"},
                {"layer": "Memory", "bottleneck": "Samsung + SK Hynix (HBM)", "risk": "Korea geopolitical risk"},
                {"layer": "Cloud", "bottleneck": "AWS + Azure + GCP (67% combined)", "risk": "Concentration risk"},
                {"layer": "Models", "bottleneck": "OpenAI + Google + Anthropic + Meta", "risk": "AI regulation"},
            ],
            "confidence": "confirmed",
        },

        "total_lobbying_spend_annual_usd": {
            "mag7_total": 82_000_000,
            "note": "Mag 7 alone spend $82M+/year lobbying US government; does not include state-level or EU lobbying",
            "confidence": "estimated",
        },

        "aggregate_insider_sales_2024_usd": {
            "bezos": 13_500_000_000,
            "zuckerberg": 1_400_000_000,
            "huang": 700_000_000,
            "cook": 50_000_000,
            "total_top_4": 15_650_000_000,
            "note": "Top 4 tech CEO/founder sales in 2024 alone = $15.65B",
            "confidence": "confirmed",
        },

        "tax_dispute_exposure_usd": {
            "microsoft_irs": 28_900_000_000,
            "apple_eu": 15_800_000_000,
            "meta_irs": 9_000_000_000,
            "total": 53_700_000_000,
            "note": "$53.7B in active tax disputes across three Mag 7 companies",
            "confidence": "confirmed",
        },
    },
}
