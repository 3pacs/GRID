"""
GRID Intelligence — Global Media, Entertainment & Information Control Network.

Static intelligence dossier covering media conglomerates, streaming platforms,
social media / information networks, financial media, and the advertising /
data brokerage complex.  Maps ownership, revenue, political influence,
content moderation controversies, market impact, and trading signals.

All data sourced from public filings (10-K, DEF 14A, Form 4, 13F),
FCC filings, DOJ / FTC antitrust filings, EU Commission decisions,
OpenSecrets.org, credible investigative journalism (FT, Reuters,
Bloomberg, NYT), and industry analytics (Nielsen, Comscore, eMarketer).

Confidence labels per GRID convention:
    confirmed  — directly from SEC filings, court rulings, or government databases
    derived    — calculated from confirmed data
    estimated  — credible third-party estimate (Bloomberg, OpenSecrets, etc.)
    rumored    — reported in media but unverified
    inferred   — pattern-detected by GRID analysis

Data vintage: public information through early 2026.
Update frequency: refresh quarterly after earnings season + after major M&A / regulatory rulings.

Key entry points:
    get_media_network()                — full network dict
    get_entity(ticker_or_id)           — single entity dossier
    get_streaming_wars()               — streaming platform sub-network
    get_social_media_network()         — social / information platforms
    get_financial_media()              — financial media entities
    get_advertising_data_complex()     — ad-tech + data brokers
    get_content_moderation_risks()     — content moderation controversy summary
    get_media_lobbying_summary()       — aggregated lobbying + PAC spend
    get_political_influence_signals()  — all political influence vectors
"""

from __future__ import annotations

from typing import Any


def get_media_network() -> dict[str, Any]:
    """Return the full media & information control network."""
    return MEDIA_NETWORK


def get_entity(ticker_or_id: str) -> dict[str, Any] | None:
    """Return dossier for a single entity by ticker or ID."""
    for section in (
        "media_conglomerates", "streaming", "social_media",
        "financial_media", "advertising_data",
    ):
        entities = MEDIA_NETWORK.get(section, {})
        if ticker_or_id in entities:
            return entities[ticker_or_id]
    return None


def get_streaming_wars() -> dict[str, Any]:
    """Return streaming platform sub-network."""
    return MEDIA_NETWORK.get("streaming", {})


def get_social_media_network() -> dict[str, Any]:
    """Return social media / information platform sub-network."""
    return MEDIA_NETWORK.get("social_media", {})


def get_financial_media() -> dict[str, Any]:
    """Return financial media entities."""
    return MEDIA_NETWORK.get("financial_media", {})


def get_advertising_data_complex() -> dict[str, Any]:
    """Return advertising & data broker sub-network."""
    return MEDIA_NETWORK.get("advertising_data", {})


def get_content_moderation_risks() -> list[dict[str, Any]]:
    """Extract content moderation controversies across all entities."""
    results = []
    for section in (
        "media_conglomerates", "streaming", "social_media",
        "financial_media", "advertising_data",
    ):
        for _id, entity in MEDIA_NETWORK.get(section, {}).items():
            cm = entity.get("content_moderation", {})
            if cm:
                results.append({
                    "entity_id": _id,
                    "entity_name": entity.get("name", _id),
                    **cm,
                })
    return results


def get_media_lobbying_summary() -> dict[str, Any]:
    """Aggregate lobbying + PAC spend across all media entities."""
    total_lobbying = 0.0
    total_pac = 0.0
    for section in (
        "media_conglomerates", "streaming", "social_media",
        "financial_media", "advertising_data",
    ):
        for _id, entity in MEDIA_NETWORK.get(section, {}).items():
            total_lobbying += entity.get("lobbying", {}).get("annual_spend_usd", 0)
            total_pac += entity.get("pac_contributions", {}).get("total_2024_cycle_usd", 0)
    return {
        "total_annual_lobbying_usd": total_lobbying,
        "total_pac_2024_cycle_usd": total_pac,
        "confidence": "estimated",
        "source": "OpenSecrets aggregation",
    }


def get_political_influence_signals() -> list[dict[str, Any]]:
    """Return all political influence vectors across media entities."""
    signals = []
    for section in (
        "media_conglomerates", "streaming", "social_media",
        "financial_media", "advertising_data",
    ):
        for _id, entity in MEDIA_NETWORK.get(section, {}).items():
            pol = entity.get("political_influence", {})
            if pol:
                signals.append({
                    "entity_id": _id,
                    "entity_name": entity.get("name", _id),
                    **pol,
                })
    return signals


# ══════════════════════════════════════════════════════════════════════════
# STATIC INTELLIGENCE DATA
# ══════════════════════════════════════════════════════════════════════════

MEDIA_NETWORK: dict[str, Any] = {
    "meta": {
        "report_type": "media_entertainment_information_control_network",
        "version": "1.0.0",
        "data_vintage": "2026-Q1",
        "refresh_cadence": "quarterly",
        "classification": "OSINT",
        "sources": [
            "SEC EDGAR (10-K, DEF 14A, Form 4, 13F)",
            "FCC filings and license databases",
            "DOJ / FTC antitrust filings",
            "EU Commission competition decisions (DMA, DSA)",
            "OpenSecrets.org (lobbying + PAC)",
            "Nielsen ratings and Comscore analytics",
            "Bloomberg Intelligence media research",
            "eMarketer / Insider Intelligence ad spending data",
            "Congressional hearing transcripts",
            "Court filings (Dominion v. Fox, DOJ v. Google, etc.)",
        ],
    },

    # ══════════════════════════════════════════════════════════════════════
    # MEDIA CONGLOMERATES
    # ══════════════════════════════════════════════════════════════════════

    "media_conglomerates": {

        # ── DISNEY (DIS) ────────────────────────────────────────────────
        "DIS": {
            "name": "The Walt Disney Company",
            "ticker": "DIS",
            "sector": "Media Conglomerate / Entertainment",
            "market_cap_usd": 200_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 225_000,
            "hq": "Burbank, CA",
            "revenue_fy2024_usd": 91_400_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024",

            "ceo": {
                "name": "Bob Iger",
                "title": "CEO",
                "appointed": "2022-11-20",
                "previous_tenure": "2005-2020",
                "total_compensation_2024_usd": 41_100_000,
                "compensation_confidence": "confirmed",
                "compensation_source": "DEF 14A proxy 2025",
                "background": "Returned from retirement to replace Bob Chapek; ABC veteran; orchestrated Pixar, Marvel, Lucasfilm, Fox acquisitions in first tenure",
                "net_worth_estimated_usd": 500_000_000,
                "net_worth_confidence": "estimated",
                "confidence": "confirmed",
            },

            "key_people": [
                {"name": "Bob Chapek", "role": "Former CEO (2020-2022)", "note": "Fired after subscriber miss + culture war backlash", "confidence": "confirmed"},
                {"name": "Dana Walden", "role": "Co-Chair Disney Entertainment", "note": "Oversees ABC, Hulu, FX, Disney+", "confidence": "confirmed"},
                {"name": "Josh D'Amaro", "role": "Chair Disney Experiences", "note": "Parks & resorts, $60B expansion plan", "confidence": "confirmed"},
            ],

            "business_segments": {
                "disney_entertainment": {
                    "revenue_usd": 41_200_000_000,
                    "includes": ["Disney+", "Hulu", "ABC", "FX", "ESPN+", "Studios (Marvel, Lucasfilm, Pixar, Disney Animation, 20th Century)"],
                    "confidence": "confirmed",
                },
                "experiences": {
                    "revenue_usd": 34_200_000_000,
                    "includes": ["Theme parks (6 resorts)", "Cruise line (expanding to 7 ships)", "Consumer products", "Licensing"],
                    "confidence": "confirmed",
                    "note": "Highest margin segment; $60B multi-year expansion announced 2023",
                },
                "espn": {
                    "revenue_usd": 16_000_000_000,
                    "includes": ["ESPN linear", "ESPN+", "ESPN Bet (Penn partnership)"],
                    "confidence": "estimated",
                    "note": "Exploring ESPN standalone streaming; Flagmantle JV with Fox and Warner abandoned after DOJ antitrust block",
                },
            },

            "streaming": {
                "disney_plus_subscribers": 150_000_000,
                "subscribers_confidence": "estimated",
                "hulu_subscribers": 50_000_000,
                "hulu_confidence": "estimated",
                "combined_streaming_losses_cumulative_usd": 11_000_000_000,
                "losses_confidence": "estimated",
                "note": "DTC segment reached profitability in Q4 FY2024 after years of losses; password sharing crackdown in progress",
                "trading_signal": "Subscriber growth deceleration vs. margin improvement is the key tension; parks CapEx provides earnings floor",
                "signal_confidence": "inferred",
            },

            "political_influence": {
                "lobbying_annual_usd": 5_700_000,
                "lobbying_confidence": "estimated",
                "lobbying_source": "OpenSecrets 2024",
                "key_issues": ["Copyright extension", "Tax incentives for film production", "Theme park permitting", "Content regulation"],
                "florida_controversy": "DeSantis vs Disney 'Don't Say Gay' feud; Reedy Creek special district battle; largely resolved 2024 with new board",
                "florida_confidence": "confirmed",
                "content_regulation_risk": "moderate",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Florida 'Don't Say Gay' backlash — both sides angry", "year": 2022, "confidence": "confirmed"},
                    {"issue": "China censorship — altered content for Chinese market", "year": "ongoing", "confidence": "confirmed"},
                    {"issue": "Star Wars / Marvel 'culture war' review bombing", "year": "ongoing", "confidence": "confirmed"},
                ],
                "risk_level": "moderate",
            },

            "market_impact": {
                "index_memberships": ["S&P 500", "DJIA"],
                "options_volume": "high",
                "earnings_mover": True,
                "earnings_impact_note": "Subscriber numbers and parks attendance drive ±5-10% post-earnings moves",
                "correlation_signals": ["Consumer discretionary spending", "Travel/tourism", "Ad market health"],
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Parks attendance + per-capita spend rising = bullish leading indicator", "confidence": "inferred"},
                {"signal": "ESPN standalone streaming launch = potential re-rating catalyst", "confidence": "inferred"},
                {"signal": "Streaming profitability inflection = sustained margin expansion", "confidence": "derived"},
            ],
        },

        # ── COMCAST / NBCUNIVERSAL (CMCSA) ─────────────────────────────
        "CMCSA": {
            "name": "Comcast Corporation",
            "ticker": "CMCSA",
            "sector": "Media Conglomerate / Telecom",
            "market_cap_usd": 155_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 186_000,
            "hq": "Philadelphia, PA",
            "revenue_fy2024_usd": 121_600_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024",

            "ceo": {
                "name": "Brian L. Roberts",
                "title": "Chairman & CEO",
                "tenure_start": 2002,
                "total_compensation_2024_usd": 34_700_000,
                "compensation_confidence": "confirmed",
                "background": "Son of founder Ralph Roberts; controls ~33% voting power through Class B shares despite owning ~1% economic interest",
                "dual_class_control": True,
                "dual_class_note": "Supervoting Class B shares give Roberts family effective veto power over any major decision",
                "net_worth_estimated_usd": 2_300_000_000,
                "net_worth_confidence": "estimated",
                "confidence": "confirmed",
            },

            "key_people": [
                {"name": "Mike Cavanagh", "role": "President, Comcast Corp", "note": "Former JPMorgan CFO; heir apparent", "confidence": "confirmed"},
                {"name": "Jeff Shell", "role": "Former CEO NBCUniversal (fired 2023)", "note": "Fired for inappropriate relationship with employee", "confidence": "confirmed"},
                {"name": "Mark Lazarus", "role": "Chairman NBCUniversal Media Group", "note": "Oversees NBC, Bravo, MSNBC, CNBC", "confidence": "confirmed"},
            ],

            "business_segments": {
                "connectivity_platforms": {
                    "revenue_usd": 65_800_000_000,
                    "includes": ["Xfinity broadband (32M subs)", "Xfinity mobile (7M+ lines)", "Business services"],
                    "confidence": "confirmed",
                    "note": "Cable video subs declining ~6-8% per year; broadband growth stalling; wireless growing",
                },
                "content_experiences": {
                    "revenue_usd": 42_000_000_000,
                    "includes": ["NBCUniversal (NBC, Telemundo, Universal Pictures, DreamWorks)", "Peacock streaming", "Theme parks (Universal Orlando, Hollywood, Japan, Beijing)"],
                    "confidence": "confirmed",
                },
                "sky": {
                    "revenue_usd": 13_800_000_000,
                    "includes": ["Sky UK", "Sky Italia", "Sky Deutschland"],
                    "confidence": "confirmed",
                    "note": "European pay-TV; acquired 2018 for $40B; declining linear subs offset by broadband",
                },
            },

            "streaming": {
                "peacock_subscribers": 36_000_000,
                "subscribers_confidence": "estimated",
                "peacock_revenue_usd": 3_900_000_000,
                "peacock_losses_2024_usd": 2_700_000_000,
                "losses_confidence": "estimated",
                "note": "Heavy sports rights investment (NFL Sunday Night, Olympics, WWE Raw); path to profitability unclear before 2026",
            },

            "cable_decline": {
                "video_subscribers_2024": 13_100_000,
                "video_subscribers_2019": 20_200_000,
                "decline_rate_annual_pct": -8,
                "broadband_threat": "FWA (T-Mobile/Verizon 5G home internet) + fiber overbuilders eroding broadband moat",
                "confidence": "derived",
            },

            "political_influence": {
                "lobbying_annual_usd": 14_100_000,
                "lobbying_confidence": "estimated",
                "key_issues": ["Net neutrality", "Broadband subsidy (ACP/BEAD)", "Media ownership rules", "Copyright"],
                "cnbc_influence": "CNBC is primary real-time financial news channel; Jim Cramer reaches millions; stock mentions create measurable short-term price impact",
                "msnbc_political_lean": "left-leaning editorial; Rachel Maddow, Morning Joe",
                "nbc_news_reach": "NBC Nightly News ~7M viewers",
                "confidence": "estimated",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "MSNBC political bias accusations", "year": "ongoing", "confidence": "confirmed"},
                    {"issue": "Matt Lauer sexual harassment scandal cover-up allegations (Ronan Farrow)", "year": 2019, "confidence": "confirmed"},
                    {"issue": "Jeff Shell CEO firing for misconduct", "year": 2023, "confidence": "confirmed"},
                ],
                "risk_level": "moderate",
            },

            "market_impact": {
                "index_memberships": ["S&P 500"],
                "earnings_mover": True,
                "earnings_impact_note": "Broadband net adds and Peacock losses are key; cord-cutting pace sets tone for entire media sector",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Broadband net adds going negative = bearish for entire cable sector (CHTR, etc.)", "confidence": "inferred"},
                {"signal": "Universal theme park expansion (Epic Universe 2025) = potential revenue catalyst", "confidence": "derived"},
                {"signal": "Potential NBCUniversal spinoff/sale rumors create event-driven opportunities", "confidence": "rumored"},
                {"signal": "CNBC Jim Cramer mentions correlate with short-term retail flow surges", "confidence": "inferred"},
            ],
        },

        # ── WARNER BROS DISCOVERY (WBD) ─────────────────────────────────
        "WBD": {
            "name": "Warner Bros. Discovery, Inc.",
            "ticker": "WBD",
            "sector": "Media Conglomerate",
            "market_cap_usd": 22_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 37_000,
            "hq": "New York, NY",
            "revenue_fy2024_usd": 39_900_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024",

            "ceo": {
                "name": "David Zaslav",
                "title": "President & CEO",
                "appointed": "2022-04-08",
                "total_compensation_2024_usd": 49_700_000,
                "compensation_confidence": "confirmed",
                "compensation_note": "Controversial pay vs. company performance — stock down ~70% since merger close",
                "background": "Former Discovery CEO; NBCUniversal / cable veteran; debt-reduction focus post-merger",
                "net_worth_estimated_usd": 600_000_000,
                "net_worth_confidence": "estimated",
                "confidence": "confirmed",
            },

            "key_people": [
                {"name": "John Stankey", "role": "AT&T CEO (engineered WarnerMedia spinoff)", "note": "AT&T took $43B loss spinning off WarnerMedia", "confidence": "confirmed"},
                {"name": "Jason Kilar", "role": "Former WarnerMedia CEO", "note": "HBO Max architect; departed post-merger", "confidence": "confirmed"},
                {"name": "Casey Bloys", "role": "Chairman & CEO HBO / Max Content", "note": "Runs HBO prestige content machine", "confidence": "confirmed"},
            ],

            "debt_burden": {
                "total_debt_usd": 41_000_000_000,
                "debt_confidence": "confirmed",
                "debt_note": "Inherited from AT&T-Discovery merger; most-indebted pure media company",
                "debt_to_ebitda_ratio": 4.2,
                "deleveraging_target": "3.0x by 2026",
                "annual_interest_expense_usd": 2_100_000_000,
                "confidence": "confirmed",
                "source": "10-K FY2024",
                "trading_signal": "Debt maturity wall 2025-2027 creates refinancing risk; credit spread widening = stock headwind",
                "signal_confidence": "derived",
            },

            "business_segments": {
                "studios": {
                    "revenue_usd": 12_800_000_000,
                    "includes": ["Warner Bros. Pictures", "New Line Cinema", "Warner Bros. Animation", "DC Studios", "Games (Hogwarts Legacy)"],
                    "confidence": "confirmed",
                },
                "networks": {
                    "revenue_usd": 20_000_000_000,
                    "includes": ["CNN", "TNT", "TBS", "Discovery Channel", "HGTV", "Food Network", "TLC"],
                    "confidence": "confirmed",
                    "note": "Linear networks declining; ad revenue under pressure; CNN ratings down significantly",
                },
                "dtc_streaming": {
                    "revenue_usd": 10_100_000_000,
                    "includes": ["Max (formerly HBO Max)", "Discovery+"],
                    "max_subscribers": 110_000_000,
                    "subscribers_confidence": "estimated",
                    "confidence": "confirmed",
                },
            },

            "cnn": {
                "viewership_decline_pct_from_2020": -50,
                "restructuring": "CEO Mark Thompson (ex-NYT) brought in 2023; pivoting to digital subscription + streaming",
                "political_lean": "center-left editorial; Chris Licht attempted rightward shift, fired 2023",
                "revenue_estimated_usd": 1_000_000_000,
                "revenue_confidence": "estimated",
                "confidence": "estimated",
            },

            "political_influence": {
                "lobbying_annual_usd": 4_800_000,
                "lobbying_confidence": "estimated",
                "key_issues": ["Sports rights antitrust", "Copyright", "Streaming regulation", "Net neutrality"],
                "cnn_political_significance": "CNN remains influential in political cycles despite ratings decline; debate hosting shapes elections",
                "confidence": "estimated",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Zaslav shelved completed films (Batgirl, Coyote vs Acme) for tax write-downs; creator backlash", "year": 2022, "confidence": "confirmed"},
                    {"issue": "CNN credibility erosion through management chaos", "year": "2022-2024", "confidence": "confirmed"},
                    {"issue": "DC reboot alienated existing fanbase", "year": 2023, "confidence": "confirmed"},
                ],
                "risk_level": "high",
            },

            "market_impact": {
                "index_memberships": ["S&P 500"],
                "earnings_mover": True,
                "credit_watch": True,
                "earnings_impact_note": "Debt covenants + streaming sub growth are what matters; box office provides upside optionality",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Credit spread widening on WBD debt = leading indicator of stock decline", "confidence": "derived"},
                {"signal": "NBA rights loss (moved to Amazon/ESPN) is structural negative for TNT", "confidence": "confirmed"},
                {"signal": "Any asset sale (CNN, gaming) = potential debt-reduction catalyst", "confidence": "inferred"},
                {"signal": "Stock trades below book value — potential breakup / acquisition target", "confidence": "inferred"},
            ],
        },

        # ── PARAMOUNT GLOBAL (PARA) ─────────────────────────────────────
        "PARA": {
            "name": "Paramount Global",
            "ticker": "PARA",
            "sector": "Media Conglomerate",
            "market_cap_usd": 8_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 22_000,
            "hq": "New York, NY",
            "revenue_fy2024_usd": 29_700_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024",

            "controlling_shareholder": {
                "name": "Shari Redstone",
                "entity": "National Amusements Inc.",
                "voting_control_pct": 77,
                "economic_interest_pct": 10,
                "dual_class_note": "Class A supervoting shares give Redstone family control despite minority economics",
                "net_worth_estimated_usd": 3_000_000_000,
                "net_worth_confidence": "estimated",
                "confidence": "confirmed",
            },

            "key_people": [
                {"name": "Bob Bakish", "role": "Former CEO (departed 2024)", "note": "Pushed out during Skydance negotiations", "confidence": "confirmed"},
                {"name": "David Ellison", "role": "Skydance Media CEO", "note": "Son of Larry Ellison (Oracle); Skydance deal to acquire Paramount", "confidence": "confirmed"},
                {"name": "Jeff Shell", "role": "Potential Paramount CEO via Skydance", "note": "Former NBCUniversal CEO", "confidence": "rumored"},
            ],

            "skydance_merger": {
                "status": "pending_regulatory_approval",
                "deal_value_usd": 28_000_000_000,
                "structure": "Skydance (David Ellison) to merge with Paramount; Shari Redstone cashed out National Amusements stake for ~$2B",
                "larry_ellison_backing": True,
                "larry_ellison_note": "Oracle co-founder Larry Ellison (~$200B net worth) bankrolling son's media ambitions",
                "regulatory_risk": "FCC + DOJ review; some concerns over broadcast license transfer",
                "previous_suitors": ["Apollo Global Management", "Sony", "Warner Bros. Discovery (informal)"],
                "confidence": "confirmed",
                "trading_signal": "Merger arb spread = key short-term signal; deal break risk creates volatility",
                "signal_confidence": "derived",
            },

            "business_segments": {
                "tv_media": {
                    "revenue_usd": 19_400_000_000,
                    "includes": ["CBS", "MTV", "Nickelodeon", "BET", "Comedy Central", "Showtime (merged into Paramount+)"],
                    "confidence": "confirmed",
                },
                "filmed_entertainment": {
                    "revenue_usd": 3_400_000_000,
                    "includes": ["Paramount Pictures", "Miramax"],
                    "confidence": "confirmed",
                },
                "dtc_streaming": {
                    "revenue_usd": 6_900_000_000,
                    "includes": ["Paramount+", "Pluto TV (FAST)"],
                    "paramount_plus_subscribers": 72_000_000,
                    "confidence": "estimated",
                },
            },

            "political_influence": {
                "lobbying_annual_usd": 3_200_000,
                "lobbying_confidence": "estimated",
                "cbs_news_influence": "CBS Evening News, 60 Minutes, Face the Nation — significant political journalism",
                "key_issues": ["Broadcast spectrum", "Content regulation", "Copyright"],
                "confidence": "estimated",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "CBS News leadership upheaval; accusations of political bias", "year": 2024, "confidence": "confirmed"},
                    {"issue": "Nickelodeon abuse allegations (Quiet on Set documentary)", "year": 2024, "confidence": "confirmed"},
                    {"issue": "MTV cultural irrelevance; brand erosion", "year": "ongoing", "confidence": "confirmed"},
                ],
                "risk_level": "high",
            },

            "trading_signals": [
                {"signal": "Skydance merger arb spread narrows = deal confidence rising", "confidence": "derived"},
                {"signal": "Deal break = massive downside given standalone Paramount financial weakness", "confidence": "inferred"},
                {"signal": "NFL rights (CBS has AFC package) provide floor value for any acquirer", "confidence": "derived"},
            ],
        },

        # ── FOX CORP (FOXA) ─────────────────────────────────────────────
        "FOXA": {
            "name": "Fox Corporation",
            "ticker": "FOXA",
            "sector": "Media / News",
            "market_cap_usd": 21_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 11_000,
            "hq": "New York, NY",
            "revenue_fy2024_usd": 14_900_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024",

            "controlling_family": {
                "family": "Murdoch",
                "key_members": [
                    {"name": "Rupert Murdoch", "role": "Chairman Emeritus", "age": 94, "note": "Stepped down as Fox chairman Nov 2023; attempted family trust restructure to give Lachlan permanent control", "confidence": "confirmed"},
                    {"name": "Lachlan Murdoch", "role": "Executive Chairman & CEO", "note": "Sole executive control post-Rupert transition; political conservative", "confidence": "confirmed"},
                    {"name": "James Murdoch", "role": "Estranged family member", "note": "Left Fox/News Corp boards 2020; runs Lupa Systems; politically moderate/liberal; opposes Fox News editorial direction", "confidence": "confirmed"},
                    {"name": "Elisabeth Murdoch", "role": "Media entrepreneur", "note": "Runs Sister (production company); politically moderate", "confidence": "confirmed"},
                    {"name": "Prudence Murdoch", "role": "Trust beneficiary", "note": "Least publicly active; sided with Lachlan in trust battle", "confidence": "estimated"},
                ],
                "trust_battle": {
                    "description": "Rupert attempted to restructure Murdoch Family Trust to give Lachlan sole control; other siblings contested in Nevada Probate Court",
                    "ruling": "Nevada commissioner ruled against Rupert's amendment in Dec 2024",
                    "status": "Appeal expected; succession remains contested",
                    "confidence": "confirmed",
                },
                "voting_control_pct": 42,
                "dual_class": True,
                "confidence": "confirmed",
            },

            "dominion_settlement": {
                "amount_usd": 787_500_000,
                "date": "2023-04-18",
                "case": "Dominion Voting Systems v. Fox News Network",
                "allegation": "Fox knowingly broadcast false 2020 election fraud claims",
                "on_air_correction": False,
                "note": "Largest known media defamation settlement in US history; Smartmatic case ($2.7B claim) still pending",
                "smartmatic_case_status": "pending_trial",
                "confidence": "confirmed",
                "source": "Court records, Delaware Superior Court",
            },

            "business_segments": {
                "cable_network_programming": {
                    "revenue_usd": 6_600_000_000,
                    "includes": ["Fox News Channel", "Fox Business Network", "FS1", "FS2"],
                    "fox_news_primetime_viewers": 2_100_000,
                    "fox_news_note": "#1 rated cable news network for 22+ consecutive years",
                    "confidence": "confirmed",
                },
                "television": {
                    "revenue_usd": 6_800_000_000,
                    "includes": ["Fox broadcast network", "Fox Sports (NFL, MLB, college football)", "Tubi (FAST streaming)"],
                    "tubi_mau": 80_000_000,
                    "tubi_confidence": "estimated",
                    "confidence": "confirmed",
                },
                "other": {
                    "revenue_usd": 1_500_000_000,
                    "includes": ["Fox Nation (subscription)", "Credible (fintech)", "Outkick"],
                    "confidence": "estimated",
                },
            },

            "political_influence": {
                "assessment": "most_politically_influential_media_entity_in_us",
                "confidence": "inferred",
                "fox_news_influence": {
                    "description": "Fox News is the primary information source for ~25-30% of US adults; disproportionate influence on Republican primary voters",
                    "primetime_hosts_2025": ["Sean Hannity", "Jesse Watters", "Laura Ingraham", "Greg Gutfeld"],
                    "fired_notable": ["Tucker Carlson (April 2023)", "Bill O'Reilly (2017)", "Megyn Kelly (departed 2017)"],
                    "tucker_carlson_note": "Fired post-Dominion settlement; launched Tucker Carlson Network; Elon Musk interview; Putin interview",
                    "confidence": "confirmed",
                },
                "lobbying_annual_usd": 5_400_000,
                "lobbying_confidence": "estimated",
                "key_issues": ["Media ownership rules", "Section 230", "Copyright", "Sports betting regulation"],
                "political_lean": "right-leaning editorial across Fox News; Fox broadcast is more centrist",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Dominion settlement — $787M for broadcasting false election claims", "year": 2023, "confidence": "confirmed"},
                    {"issue": "Smartmatic $2.7B defamation case pending", "year": "pending", "confidence": "confirmed"},
                    {"issue": "Internal communications revealed hosts privately doubted claims they broadcast", "year": 2023, "confidence": "confirmed"},
                    {"issue": "Fox News host sexual harassment scandals (O'Reilly, Ailes)", "year": "2016-2017", "confidence": "confirmed"},
                ],
                "risk_level": "critical",
            },

            "market_impact": {
                "index_memberships": ["S&P 500"],
                "earnings_mover": True,
                "earnings_impact_note": "Political ad spending in election years drives revenue spikes; sports rights renewals are multi-year catalysts",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Election year = political ad revenue surge; Fox benefits disproportionately", "confidence": "derived"},
                {"signal": "Smartmatic trial outcome = significant binary risk ($2.7B claim)", "confidence": "confirmed"},
                {"signal": "Murdoch succession resolution could trigger strategic review / breakup", "confidence": "inferred"},
                {"signal": "Tubi growth as FAST (free ad-supported) hedge against cord-cutting", "confidence": "inferred"},
            ],
        },

        # ── SONY GROUP (SONY) ──────────────────────────────────────────
        "SONY": {
            "name": "Sony Group Corporation",
            "ticker": "SONY",
            "sector": "Media Conglomerate / Electronics / Gaming",
            "market_cap_usd": 115_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 113_000,
            "hq": "Tokyo, Japan (US: New York, NY)",
            "revenue_fy2024_usd": 88_000_000_000,
            "revenue_confidence": "estimated",
            "revenue_note": "JPY-denominated; USD equivalent at ~¥150/USD",

            "ceo": {
                "name": "Kenichiro Yoshida",
                "title": "Chairman, President & CEO",
                "appointed": 2018,
                "background": "Finance / CFO background; architect of Sony's pivot from electronics to content IP (music, gaming, film)",
                "total_compensation_2024_usd": 7_800_000,
                "compensation_confidence": "estimated",
                "compensation_note": "Japanese CEO pay is significantly lower than US peers",
                "confidence": "confirmed",
            },

            "key_people": [
                {"name": "Tony Vinciquerra", "role": "Former Chairman Sony Pictures (retired 2024)", "confidence": "confirmed"},
                {"name": "Ravi Ahuja", "role": "Chairman & CEO Sony Pictures Entertainment", "confidence": "confirmed"},
                {"name": "Hiroki Totoki", "role": "President, COO & CFO", "confidence": "confirmed"},
                {"name": "Rob Stringer", "role": "Chairman Sony Music Group", "note": "Largest music company by revenue", "confidence": "confirmed"},
            ],

            "business_segments": {
                "game_network_services": {
                    "revenue_usd": 29_000_000_000,
                    "includes": ["PlayStation 5", "PlayStation Network", "PlayStation Plus (47M subs)", "First-party studios", "Bungie"],
                    "ps5_installed_base": 65_000_000,
                    "confidence": "estimated",
                },
                "music": {
                    "revenue_usd": 12_500_000_000,
                    "includes": ["Sony Music Entertainment", "Sony/ATV Music Publishing (merged into Sony Music Publishing)"],
                    "note": "World's largest music publisher; artists: Beyonce, Adele, Harry Styles, Bad Bunny",
                    "streaming_growth": "Spotify + Apple Music growth drives recurring royalties",
                    "confidence": "estimated",
                },
                "pictures": {
                    "revenue_usd": 11_000_000_000,
                    "includes": ["Columbia Pictures", "TriStar", "Sony Pictures Animation", "Crunchyroll (anime streaming, 15M subs)"],
                    "spider_man_note": "Spider-Man / Marvel deal is highest-value single-character IP in film",
                    "confidence": "estimated",
                },
                "imaging_sensing": {
                    "revenue_usd": 12_000_000_000,
                    "includes": ["Image sensors (CMOS)", "Camera products"],
                    "market_share": "~50% of global smartphone image sensors (supplies Apple, Samsung)",
                    "confidence": "estimated",
                },
            },

            "political_influence": {
                "lobbying_annual_usd": 4_200_000,
                "lobbying_confidence": "estimated",
                "lobbying_note": "Sony Pictures US lobbying; separate from Japan corporate lobbying",
                "key_issues": ["Copyright protection (anti-piracy)", "Trade policy (Japan-US)", "Content regulation", "Gaming loot box regulation"],
                "confidence": "estimated",
            },

            "market_impact": {
                "dual_listed": True,
                "primary_listing": "TYO: 6758",
                "us_listing": "NYSE: SONY (ADR)",
                "index_memberships": ["Nikkei 225", "TOPIX"],
                "yen_sensitivity": "Strong USD = headwind for yen-denominated revenue translated to USD",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "PS5 Pro launch cycle + GTA VI exclusive content could drive gaming segment", "confidence": "inferred"},
                {"signal": "Music streaming growth is most predictable revenue stream; Spotify sub growth = Sony royalty growth", "confidence": "derived"},
                {"signal": "Anime (Crunchyroll) is fastest-growing content vertical globally", "confidence": "inferred"},
                {"signal": "Image sensor dominance makes Sony a stealth AI play (computer vision, autonomous vehicles)", "confidence": "inferred"},
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # STREAMING WARS
    # ══════════════════════════════════════════════════════════════════════

    "streaming": {

        # ── NETFLIX (NFLX) ──────────────────────────────────────────────
        "NFLX": {
            "name": "Netflix, Inc.",
            "ticker": "NFLX",
            "sector": "Streaming / Entertainment",
            "market_cap_usd": 400_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 13_000,
            "hq": "Los Gatos, CA",
            "revenue_fy2024_usd": 39_000_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024",

            "leadership": {
                "co_ceo": {
                    "name": "Ted Sarandos",
                    "title": "Co-CEO",
                    "background": "Former Chief Content Officer; dealmaker behind Netflix original content strategy",
                    "total_compensation_2024_usd": 49_800_000,
                    "compensation_confidence": "confirmed",
                    "confidence": "confirmed",
                },
                "co_ceo_2": {
                    "name": "Greg Peters",
                    "title": "Co-CEO",
                    "background": "Former COO; product / tech / ad-tier architect",
                    "total_compensation_2024_usd": 45_100_000,
                    "compensation_confidence": "confirmed",
                    "confidence": "confirmed",
                },
                "founder": {
                    "name": "Reed Hastings",
                    "title": "Executive Chairman",
                    "net_worth_estimated_usd": 6_500_000_000,
                    "net_worth_confidence": "estimated",
                    "note": "Stepped down as co-CEO Jan 2023; major philanthropist (education)",
                    "confidence": "confirmed",
                },
            },

            "subscribers": {
                "global_paid": 301_000_000,
                "global_confidence": "confirmed",
                "note": "Stopped reporting quarterly sub numbers Q1 2025; shifted to revenue/ARM focus",
                "ad_tier_subscribers": 70_000_000,
                "ad_tier_confidence": "estimated",
                "password_sharing_crackdown": {
                    "launched": "2023-Q2",
                    "impact": "Added ~30M+ net subs in 12 months post-crackdown",
                    "confidence": "derived",
                },
            },

            "business_model": {
                "subscription_revenue_usd": 36_000_000_000,
                "ad_revenue_usd": 3_000_000_000,
                "ad_revenue_confidence": "estimated",
                "ad_revenue_note": "Ad tier launched Nov 2022; growing rapidly but still small vs. total",
                "content_spend_annual_usd": 17_000_000_000,
                "content_confidence": "confirmed",
                "live_events": "WWE Raw (starting 2025, $5B/10yr), NFL Christmas games, live comedy specials",
                "gaming": "40+ mobile games; nascent but growing; strategy unclear",
                "confidence": "confirmed",
            },

            "competitive_position": {
                "moat": "Scale (300M+ subs) + content library + recommendation algorithm + global distribution",
                "key_advantage": "Only profitable pure-play streamer at scale",
                "threats": ["Content cost inflation", "Sports rights arms race", "Ad tier cannibalization of premium tier", "Regulatory (EU content quotas)"],
                "confidence": "inferred",
            },

            "political_influence": {
                "lobbying_annual_usd": 1_200_000,
                "lobbying_confidence": "estimated",
                "key_issues": ["Content regulation internationally", "Data privacy (viewing data)", "Net neutrality", "Trade (content access in markets like India, Korea)"],
                "cultural_influence": "Massive — Netflix originals shape cultural conversation globally; Squid Game, Wednesday, Stranger Things reach >100M households",
                "confidence": "estimated",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Dave Chappelle trans jokes backlash + employee walkout", "year": 2021, "confidence": "confirmed"},
                    {"issue": "'Cuties' controversy — accusations of sexualizing minors", "year": 2020, "confidence": "confirmed"},
                    {"issue": "Cleopatra documentary racial controversy", "year": 2023, "confidence": "confirmed"},
                    {"issue": "Ted Sarandos 'artistic freedom' stance alienated some employees", "year": 2021, "confidence": "confirmed"},
                ],
                "risk_level": "moderate",
            },

            "market_impact": {
                "index_memberships": ["S&P 500", "NASDAQ-100"],
                "options_volume": "very_high",
                "earnings_mover": True,
                "earnings_impact_note": "Revenue growth + margin expansion drive ±10-15% post-earnings moves; stopped reporting sub counts, making revenue the sole focus",
                "sector_bellwether": True,
                "sector_note": "Netflix earnings set tone for entire streaming/media sector for the quarter",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Ad tier revenue acceleration = multiple expansion catalyst", "confidence": "inferred"},
                {"signal": "ARM (average revenue per member) growth > subscriber growth = margin expansion story", "confidence": "derived"},
                {"signal": "WWE Raw ratings on Netflix = indicator of live event strategy success", "confidence": "inferred"},
                {"signal": "Netflix is inverse-correlated with theatrical box office during weak movie slates", "confidence": "inferred"},
            ],
        },

        # ── AMAZON PRIME VIDEO (AMZN) ───────────────────────────────────
        "AMZN_PRIME": {
            "name": "Amazon Prime Video (Amazon.com, Inc.)",
            "ticker": "AMZN",
            "sector": "Streaming / E-commerce / Cloud",
            "parent_market_cap_usd": 2_100_000_000_000,
            "market_cap_confidence": "estimated",

            "leadership": {
                "name": "Andy Jassy",
                "title": "CEO, Amazon",
                "prime_video_head": "Mike Hopkins (SVP Prime Video & Amazon Studios)",
                "confidence": "confirmed",
            },

            "prime_video": {
                "prime_members_global": 200_000_000,
                "members_confidence": "estimated",
                "note": "Prime Video bundled with Amazon Prime membership ($14.99/mo); impossible to separate video-only subs",
                "content_spend_annual_usd": 12_000_000_000,
                "content_confidence": "estimated",
                "content_note": "Includes Lord of the Rings: Rings of Power ($465M S1), Thursday Night Football ($1B/yr), MGM library",
                "mgm_acquisition": {
                    "completed": "2022-03-17",
                    "price_usd": 8_500_000_000,
                    "assets_acquired": "MGM library (4,000+ films: James Bond, Rocky, etc.), Epix",
                    "confidence": "confirmed",
                },
                "ad_tier": {
                    "launched": "2024-01-29",
                    "description": "Ads inserted into all Prime Video by default; $2.99/mo to remove ads",
                    "controversy": "Backlash for inserting ads into existing paid service",
                    "projected_ad_revenue_usd": 5_000_000_000,
                    "ad_revenue_confidence": "estimated",
                },
                "live_sports": ["Thursday Night Football (11yr/$11B)", "NBA (11yr, starting 2025)", "Champions League (UK)", "NASCAR"],
                "confidence": "estimated",
            },

            "strategic_purpose": {
                "description": "Prime Video exists to reduce Prime membership churn and increase shopping frequency; content is a loss leader for e-commerce",
                "prime_churn_reduction": "Video content reduces Prime churn by estimated 25%",
                "prime_member_spend": "Prime members spend ~$1,400/yr on Amazon vs ~$600 for non-members",
                "confidence": "inferred",
            },

            "political_influence": {
                "note": "See AMZN in tech_monopoly_network.py for Amazon's full political influence profile",
                "media_specific_lobbying": "Sports rights negotiations, content regulation, international market access",
                "confidence": "estimated",
            },

            "trading_signals": [
                {"signal": "Prime sub growth = bullish for both retail and streaming", "confidence": "derived"},
                {"signal": "Ad tier revenue ramp is underappreciated by market (priced into AWS, not video)", "confidence": "inferred"},
                {"signal": "Live sports acquisition = long-term content cost lock-in advantage", "confidence": "inferred"},
            ],
        },

        # ── APPLE TV+ (AAPL) ───────────────────────────────────────────
        "AAPL_TV": {
            "name": "Apple TV+ (Apple Inc.)",
            "ticker": "AAPL",
            "sector": "Streaming / Consumer Electronics",
            "parent_market_cap_usd": 3_400_000_000_000,
            "market_cap_confidence": "estimated",

            "apple_tv_plus": {
                "launched": "2019-11-01",
                "subscribers_estimated": 45_000_000,
                "subscribers_confidence": "estimated",
                "subscribers_note": "Apple does not disclose; many on free trials or bundled with Apple One",
                "content_spend_annual_usd": 9_000_000_000,
                "content_confidence": "estimated",
                "content_note": "Shifted from $1B/yr initial reports to ~$9B including sports (MLS, Friday Night Baseball, potential NFL/NBA bids)",
                "revenue_estimated_usd": 4_000_000_000,
                "revenue_confidence": "estimated",
                "profit_loss": "Substantial annual losses; estimates range from $1-5B net loss on streaming alone",
                "loss_confidence": "estimated",
            },

            "strategic_purpose": {
                "description": "Apple TV+ is a loss leader to keep users in Apple ecosystem (1.5B active devices); services margin is 70%+",
                "services_bundle": "Apple One bundle ($19.95/mo) includes TV+, Music, Arcade, iCloud, News+, Fitness+",
                "halo_effect": "Award-winning content (Severance, Ted Lasso, Killers of the Flower Moon) burnishes Apple brand premium",
                "confidence": "inferred",
            },

            "notable_content": [
                "Ted Lasso (cultural phenomenon)",
                "Severance (critical darling)",
                "Killers of the Flower Moon (Scorsese, $200M budget)",
                "The Morning Show (Jennifer Aniston, Reese Witherspoon)",
                "MLS Season Pass ($2.5B/10yr)",
            ],

            "trading_signals": [
                {"signal": "Apple TV+ content wins (Oscars, Emmys) do NOT move AAPL stock — too small relative to $3.4T market cap", "confidence": "confirmed"},
                {"signal": "Services revenue growth rate is what matters for AAPL; TV+ is rounding error", "confidence": "derived"},
                {"signal": "Potential NFL/NBA rights bid would signal Apple getting serious about live sports moat", "confidence": "inferred"},
            ],
        },

        # ── YOUTUBE / GOOGLE (GOOGL) ───────────────────────────────────
        "GOOGL_YT": {
            "name": "YouTube (Alphabet Inc.)",
            "ticker": "GOOGL",
            "sector": "Streaming / Advertising / Creator Economy",
            "parent_market_cap_usd": 2_200_000_000_000,
            "market_cap_confidence": "estimated",

            "leadership": {
                "youtube_ceo": {
                    "name": "Neal Mohan",
                    "title": "CEO, YouTube",
                    "appointed": "2023-02-16",
                    "background": "Former YouTube CPO; display advertising veteran; joined Google via DoubleClick acquisition",
                    "confidence": "confirmed",
                },
                "former_ceo": {
                    "name": "Susan Wojcicki",
                    "role": "Former CEO (2014-2023)",
                    "note": "Passed away August 2024",
                    "confidence": "confirmed",
                },
            },

            "financials": {
                "ad_revenue_2024_usd": 36_800_000_000,
                "ad_revenue_confidence": "confirmed",
                "ad_revenue_source": "Alphabet 10-K FY2024",
                "youtube_premium_music_subscribers": 100_000_000,
                "subscribers_confidence": "estimated",
                "youtube_tv_subscribers": 8_000_000,
                "youtube_tv_confidence": "estimated",
                "youtube_tv_note": "Largest US live TV streaming service; $72.99/mo",
                "total_youtube_revenue_estimated_usd": 45_000_000_000,
                "total_revenue_note": "Includes ads + subscriptions (Premium, Music, TV) + Super Chat + Channel Memberships",
                "total_confidence": "estimated",
            },

            "creator_economy": {
                "total_creators_monetized": 3_000_000,
                "creator_payouts_annual_usd": 16_000_000_000,
                "creator_revenue_share": "55% to creators on AdSense; 70% on Shorts (competing with TikTok)",
                "shorts_daily_views": 70_000_000_000,
                "shorts_confidence": "estimated",
                "mr_beast_note": "Jimmy Donaldson (MrBeast) — largest individual creator (300M+ subs); indicator of platform health",
                "confidence": "estimated",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Child safety (COPPA violations, predatory comments on kids' content)", "year": "ongoing", "confidence": "confirmed"},
                    {"issue": "Misinformation spread (COVID, election, conspiracy theories)", "year": "ongoing", "confidence": "confirmed"},
                    {"issue": "Adpocalypse events — advertiser boycotts over brand safety", "year": "2017-ongoing", "confidence": "confirmed"},
                    {"issue": "AI-generated deepfake content moderation challenges", "year": "2024-ongoing", "confidence": "confirmed"},
                ],
                "risk_level": "high",
            },

            "market_impact": {
                "note": "YouTube revenue reported as segment within Alphabet; see GOOGL in tech_monopoly_network.py",
                "sentiment_signal": "YouTube financial content (Meet Kevin, Graham Stephan) influences retail investor behavior",
                "creator_economy_signal": "YouTube ad revenue growth is leading indicator of broader digital ad market health",
                "confidence": "inferred",
            },

            "trading_signals": [
                {"signal": "YouTube ad revenue growth rate vs Meta/TikTok = competitive positioning signal", "confidence": "derived"},
                {"signal": "Shorts monetization ramp = key to defending against TikTok", "confidence": "inferred"},
                {"signal": "YouTube TV subscriber growth = cord-cutting beneficiary", "confidence": "derived"},
                {"signal": "Creator economy health (payouts, new partner program members) = platform moat indicator", "confidence": "inferred"},
            ],
        },

        # ── SPOTIFY (SPOT) ─────────────────────────────────────────────
        "SPOT": {
            "name": "Spotify Technology S.A.",
            "ticker": "SPOT",
            "sector": "Music Streaming / Podcasting / Audio",
            "market_cap_usd": 100_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 9_000,
            "hq": "Stockholm, Sweden (listed NYSE)",
            "revenue_fy2024_usd": 16_000_000_000,
            "revenue_confidence": "estimated",
            "revenue_note": "EUR-denominated; ~EUR 14.7B",

            "ceo": {
                "name": "Daniel Ek",
                "title": "Co-Founder & CEO",
                "net_worth_estimated_usd": 8_000_000_000,
                "net_worth_confidence": "estimated",
                "background": "Swedish entrepreneur; founded Spotify 2006; long battle with music labels for better economics",
                "confidence": "confirmed",
            },

            "subscribers": {
                "premium_subscribers": 252_000_000,
                "premium_confidence": "confirmed",
                "monthly_active_users": 675_000_000,
                "mau_confidence": "confirmed",
                "note": "Largest audio streaming platform globally; ~31% premium conversion rate",
            },

            "business_segments": {
                "premium_subscriptions": {
                    "revenue_usd": 14_000_000_000,
                    "margin_trend": "Improving — first sustained profitability in 2024 after years of losses",
                    "price_increases": "Multiple increases 2023-2025 ($10.99 → $11.99 → $12.99 in US); reduced churn fears unfounded",
                    "confidence": "estimated",
                },
                "advertising": {
                    "revenue_usd": 2_000_000_000,
                    "includes": ["Music ad-supported tier", "Podcast advertising", "Spotify Ad Studio"],
                    "confidence": "estimated",
                },
                "podcasting": {
                    "status": "Strategic pivot — moved from exclusive licensing ($1B+ spent on Rogan, Gimlet, Ringer) to open ecosystem",
                    "joe_rogan_deal": "$250M exclusive (2024 renewal for $250M; now non-exclusive on video)",
                    "losses_on_podcast_investments": "Wrote off ~$1B in podcast goodwill (Gimlet, Parcast, etc.)",
                    "confidence": "confirmed",
                },
            },

            "label_economics": {
                "royalty_payout_pct": 70,
                "annual_royalty_payouts_usd": 9_000_000_000,
                "major_label_dependence": "Universal Music (UMG), Sony Music, Warner Music Group control ~65% of streams",
                "artist_pay_controversy": "~$0.003-0.005 per stream; artists argue this is exploitative",
                "confidence": "estimated",
            },

            "political_influence": {
                "lobbying_annual_usd": 800_000,
                "lobbying_confidence": "estimated",
                "key_issues": ["Copyright royalty rates", "EU digital markets regulation", "Podcast content moderation"],
                "cultural_influence": "Spotify Wrapped is annual cultural event; playlist placement shapes music industry",
                "confidence": "estimated",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Joe Rogan COVID misinformation — Neil Young/Joni Mitchell boycott", "year": 2022, "confidence": "confirmed"},
                    {"issue": "AI-generated music flooding platform (fake Drake/Weeknd)", "year": 2023, "confidence": "confirmed"},
                    {"issue": "Podcast misinformation moderation inconsistency", "year": "ongoing", "confidence": "confirmed"},
                ],
                "risk_level": "moderate",
            },

            "trading_signals": [
                {"signal": "Price increases without churn acceleration = pricing power = margin expansion", "confidence": "derived"},
                {"signal": "Premium sub growth deceleration in developed markets; growth now from emerging markets (lower ARPU)", "confidence": "derived"},
                {"signal": "Audiobooks + AI DJ features = new product surface area for monetization", "confidence": "inferred"},
                {"signal": "Spotify profitability inflection (2024) = potential re-rating from 'growth' to 'growth + profit' multiple", "confidence": "inferred"},
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # SOCIAL MEDIA / INFORMATION PLATFORMS
    # ══════════════════════════════════════════════════════════════════════

    "social_media": {

        # ── META PLATFORMS (META) ───────────────────────────────────────
        "META": {
            "name": "Meta Platforms, Inc.",
            "ticker": "META",
            "sector": "Social Media / Advertising / XR",
            "market_cap_usd": 1_600_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 72_000,
            "hq": "Menlo Park, CA",
            "revenue_fy2024_usd": 164_700_000_000,
            "revenue_confidence": "confirmed",
            "revenue_source": "10-K FY2024",

            "ceo": {
                "name": "Mark Zuckerberg",
                "title": "Founder, Chairman & CEO",
                "voting_control_pct": 61,
                "dual_class": True,
                "net_worth_estimated_usd": 210_000_000_000,
                "net_worth_confidence": "estimated",
                "total_compensation_2024_usd": 32_000_000,
                "compensation_note": "Notional; Zuck takes $1 salary + security costs covered by Meta",
                "security_costs_annual_usd": 27_000_000,
                "background": "Founded Facebook at Harvard (2004); controls company absolutely via supervoting shares",
                "confidence": "confirmed",
            },

            "platforms": {
                "facebook": {
                    "dau": 2_100_000_000,
                    "mau": 3_100_000_000,
                    "note": "Still growing globally; US/EU flat; growth from Asia, Africa, Latin America",
                    "confidence": "confirmed",
                },
                "instagram": {
                    "mau": 2_500_000_000,
                    "note": "Primary growth + engagement driver; Reels competing with TikTok",
                    "confidence": "estimated",
                },
                "whatsapp": {
                    "mau": 2_800_000_000,
                    "note": "Dominant messaging in India, Brazil, Europe; monetization via WhatsApp Business API",
                    "business_api_revenue": "Growing rapidly; click-to-WhatsApp ads are fastest-growing ad format",
                    "confidence": "estimated",
                },
                "threads": {
                    "mau": 300_000_000,
                    "note": "Twitter/X competitor launched July 2023; rapid initial growth, stabilizing; no ads yet",
                    "confidence": "estimated",
                },
                "family_of_apps_total_users": 3_300_000_000,
                "total_confidence": "confirmed",
            },

            "reality_labs": {
                "annual_losses_usd": 16_000_000_000,
                "losses_confidence": "confirmed",
                "cumulative_investment_usd": 60_000_000_000,
                "includes": ["Quest VR headsets", "Ray-Ban Meta smart glasses", "Horizon Worlds", "AR glasses (Orion prototype)"],
                "zuckerberg_conviction": "Zuck has committed to spending $15-20B/yr on metaverse/AR/VR indefinitely",
                "confidence": "confirmed",
            },

            "ai_strategy": {
                "llama_model": "Open-source LLM (Llama 3.1, 405B params); most-used open AI model",
                "ai_capex_2025_usd": 60_000_000_000,
                "capex_confidence": "confirmed",
                "ai_applications": ["Feed/Reels recommendation", "Ad targeting", "Meta AI assistant", "Content moderation automation"],
                "confidence": "confirmed",
            },

            "political_influence": {
                "lobbying_annual_usd": 19_400_000,
                "lobbying_confidence": "estimated",
                "lobbying_source": "OpenSecrets 2024",
                "key_issues": ["Section 230", "Privacy regulation (state + federal)", "AI regulation", "Antitrust (FTC case)", "EU DMA/DSA compliance"],
                "ftc_antitrust_case": {
                    "status": "Ongoing — FTC suing to break up Meta (divest Instagram + WhatsApp)",
                    "trial_expected": "2025-2026",
                    "potential_impact": "Breakup would be transformative for social media landscape",
                    "confidence": "confirmed",
                },
                "election_influence": "Facebook/Instagram are primary vectors for political advertising; 2016/2020 election interference controversies",
                "cambridge_analytica": "2018 scandal — 87M user profiles harvested; $5B FTC fine",
                "confidence": "confirmed",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Cambridge Analytica — 87M profiles harvested for political targeting", "year": 2018, "confidence": "confirmed"},
                    {"issue": "Myanmar genocide — UN found Facebook played 'determining role' in inciting violence", "year": 2018, "confidence": "confirmed"},
                    {"issue": "Instagram mental health impact on teens (Frances Haugen whistleblower)", "year": 2021, "confidence": "confirmed"},
                    {"issue": "2024 Zuckerberg pivot — reduced content moderation, ended fact-checking program", "year": 2024, "confidence": "confirmed"},
                    {"issue": "Election misinformation across all platforms", "year": "ongoing", "confidence": "confirmed"},
                ],
                "moderation_policy_shift_2024": "Zuckerberg publicly moved toward 'free expression' stance; ended third-party fact-checking; reduced content moderation staff; widely seen as political alignment with incoming Trump administration",
                "risk_level": "critical",
            },

            "market_impact": {
                "index_memberships": ["S&P 500", "NASDAQ-100"],
                "options_volume": "extreme",
                "earnings_mover": True,
                "earnings_impact_note": "Ad revenue growth + user engagement metrics drive ±10-20% post-earnings moves",
                "ad_market_bellwether": True,
                "ad_note": "Meta ad revenue is single best real-time indicator of digital advertising health",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Meta ad revenue growth = leading indicator of overall digital ad market", "confidence": "confirmed"},
                {"signal": "Reality Labs losses are priced in; any reduction = upside surprise", "confidence": "inferred"},
                {"signal": "FTC breakup trial timeline creates binary event risk 2025-2026", "confidence": "derived"},
                {"signal": "Threads monetization (ads) = potential $5-10B revenue opportunity", "confidence": "inferred"},
                {"signal": "Reels engagement vs. TikTok = competitive positioning signal", "confidence": "inferred"},
            ],
        },

        # ── X / TWITTER ─────────────────────────────────────────────────
        "X_TWITTER": {
            "name": "X Corp (formerly Twitter, Inc.)",
            "ticker": None,
            "sector": "Social Media / Information",
            "market_cap_usd": None,
            "ownership": "private",
            "hq": "San Francisco, CA (also Austin, TX)",

            "owner": {
                "name": "Elon Musk",
                "acquisition_date": "2022-10-27",
                "acquisition_price_usd": 44_000_000_000,
                "current_valuation_estimated_usd": 19_000_000_000,
                "valuation_confidence": "estimated",
                "valuation_note": "Fidelity marked down Twitter stake ~65% from purchase price; xAI equity swap in 2025 complicates valuation",
                "net_worth_estimated_usd": 330_000_000_000,
                "net_worth_confidence": "estimated",
                "net_worth_note": "Primarily from Tesla (TSLA) + SpaceX; world's richest person (as of early 2026)",
                "financing": {
                    "equity_from_musk_usd": 27_000_000_000,
                    "bank_debt_usd": 13_000_000_000,
                    "co_investors": ["Larry Ellison ($1B)", "Sequoia Capital ($800M)", "Binance ($500M)", "a16z ($400M)", "Qatar Investment Authority ($375M)", "Saudi Prince Alwaleed (rolled over $1.9B stake)"],
                    "confidence": "confirmed",
                },
                "confidence": "confirmed",
            },

            "key_people": [
                {"name": "Linda Yaccarino", "role": "CEO (hired June 2023)", "note": "Former NBCUniversal ad sales chief; brought in to repair advertiser relationships", "confidence": "confirmed"},
                {"name": "Elon Musk", "role": "Owner / 'Chief Twit' / de facto decision maker", "note": "Yaccarino's authority appears limited; Musk makes key product/policy decisions", "confidence": "inferred"},
            ],

            "financials": {
                "revenue_2024_estimated_usd": 3_000_000_000,
                "revenue_confidence": "estimated",
                "revenue_note": "Down ~60% from ~$5.1B pre-acquisition (2021); advertiser exodus",
                "ad_revenue_decline": "Major brands (Apple, Disney, IBM, etc.) paused or reduced spend post-Musk; many have partially returned",
                "subscription_revenue": "X Premium (verified checkmark) — $8/mo; limited adoption",
                "debt_service_annual_usd": 1_200_000_000,
                "debt_confidence": "estimated",
                "profitability": "Likely unprofitable; Musk claims 'near breakeven' but no audited financials",
                "profitability_confidence": "rumored",
            },

            "platform_metrics": {
                "mau_estimated": 600_000_000,
                "mau_confidence": "estimated",
                "mau_note": "Musk claims much higher; third-party estimates (Similarweb) suggest ~550-600M",
                "dau_estimated": 250_000_000,
                "dau_confidence": "estimated",
                "notable_user_loss": "Many journalists, academics, and brands migrated to Threads, Bluesky, or Mastodon",
            },

            "political_influence": {
                "assessment": "outsized_political_influence_relative_to_size",
                "confidence": "confirmed",
                "details": {
                    "musk_political_role": "Elon Musk became major political actor 2024; spent $250M+ supporting Trump campaign; appointed to lead DOGE (Department of Government Efficiency)",
                    "doge_note": "DOGE is unofficial government efficiency initiative; Musk used X platform to promote it",
                    "musk_spending_confidence": "estimated",
                    "digital_town_square": "X remains primary platform for real-time political discourse, breaking news, and crisis communication",
                    "journalist_dependency": "Despite exodus, most major journalists still use X for breaking news distribution",
                    "government_use": "World leaders, government agencies, and military still use X for official communications",
                },
                "key_controversies": [
                    "Reinstating banned accounts (Trump, Kanye West, Andrew Tate)",
                    "Removing legacy verification (blue checks) — verification now pay-to-play",
                    "Amplifying right-wing content through algorithm changes",
                    "Musk personally engaging in political content/conspiracy theories",
                    "Community Notes replacing traditional content moderation",
                    "Advertiser boycott following Musk's antisemitic content endorsement (Nov 2023)",
                ],
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Musk gutted trust & safety team (80% reduction)", "year": 2022, "confidence": "confirmed"},
                    {"issue": "Antisemitic content endorsement by Musk ('the actual truth')", "year": 2023, "confidence": "confirmed"},
                    {"issue": "Hate speech increase 3x post-acquisition per multiple researchers", "year": "2022-ongoing", "confidence": "estimated"},
                    {"issue": "EU DSA investigation for content moderation failures", "year": 2024, "confidence": "confirmed"},
                    {"issue": "Brazil ban (2024) for refusing to comply with court orders", "year": 2024, "confidence": "confirmed"},
                    {"issue": "Australia e-safety commissioner clash", "year": 2024, "confidence": "confirmed"},
                ],
                "risk_level": "critical",
            },

            "market_impact": {
                "direct_market_impact": "Not publicly traded",
                "indirect_impact": "Extremely high — Musk tweets still move TSLA, crypto, and any stock he mentions",
                "musk_tweet_effect": {
                    "description": "Single Musk tweet/post can move billions in market cap within minutes",
                    "examples": ["TSLA stock moves", "Dogecoin pumps", "Signal app confusion with Advance Auto Parts (SIGNA)", "GameStop tweets"],
                    "confidence": "confirmed",
                },
                "fintwit": "Financial Twitter ('FinTwit') remains most important real-time market discussion platform",
            },

            "trading_signals": [
                {"signal": "Musk X posts about specific companies/crypto = immediate price impact (trade the reaction)", "confidence": "confirmed"},
                {"signal": "X ad revenue recovery = signal for digital ad market health", "confidence": "inferred"},
                {"signal": "X financial health affects Musk's need to sell TSLA shares (collateral)", "confidence": "inferred"},
                {"signal": "xAI valuation ($50B+) may eventually subsume X; watch for merger/restructuring", "confidence": "rumored"},
            ],
        },

        # ── TIKTOK / BYTEDANCE ──────────────────────────────────────────
        "TIKTOK": {
            "name": "TikTok (ByteDance Ltd.)",
            "ticker": None,
            "sector": "Social Media / Short Video",
            "ownership": "private (ByteDance — Beijing, China)",
            "hq": "Los Angeles, CA (US operations); Singapore (global HQ); Beijing (ByteDance parent)",

            "bytedance": {
                "valuation_usd": 225_000_000_000,
                "valuation_confidence": "estimated",
                "valuation_note": "Secondary market transactions in 2024; makes ByteDance one of world's most valuable private companies",
                "revenue_2024_usd": 120_000_000_000,
                "revenue_confidence": "estimated",
                "revenue_note": "Total ByteDance revenue including Douyin (China TikTok) and other products",
                "founder": {
                    "name": "Zhang Yiming",
                    "role": "Founder (stepped down as CEO 2021)",
                    "net_worth_estimated_usd": 49_000_000_000,
                    "net_worth_confidence": "estimated",
                },
                "ceo": {"name": "Liang Rubo", "title": "CEO, ByteDance", "confidence": "confirmed"},
                "tiktok_ceo": {"name": "Shou Chew", "title": "CEO, TikTok", "note": "Singaporean; Congressional testimony March 2023", "confidence": "confirmed"},
            },

            "tiktok_us": {
                "mau_us": 170_000_000,
                "mau_global": 1_500_000_000,
                "mau_confidence": "estimated",
                "revenue_us_estimated_usd": 16_000_000_000,
                "revenue_confidence": "estimated",
                "ad_revenue_share": "Rapidly gaining share from Meta and Google; estimated 3-4% of US digital ad market",
                "tiktok_shop": {
                    "gmv_us_2024_usd": 9_000_000_000,
                    "gmv_confidence": "estimated",
                    "description": "In-app e-commerce; direct threat to Amazon, Shopify, Instagram Shopping",
                },
            },

            "us_ban_saga": {
                "timeline": [
                    {"date": "2020-08", "event": "Trump executive order to ban TikTok; Oracle/Walmart deal proposed", "confidence": "confirmed"},
                    {"date": "2021-06", "event": "Biden revoked Trump order; initiated own national security review", "confidence": "confirmed"},
                    {"date": "2024-03", "event": "House passed forced-sale bill (Protecting Americans from Foreign Adversary Controlled Applications Act)", "confidence": "confirmed"},
                    {"date": "2024-04", "event": "Senate passed bill; Biden signed into law; ByteDance given ~270 days to divest or face ban", "confidence": "confirmed"},
                    {"date": "2025-01", "event": "Supreme Court upheld ban law as constitutional", "confidence": "confirmed"},
                    {"date": "2025-01-19", "event": "TikTok briefly went dark in US; Trump executive order granted 75-day extension", "confidence": "confirmed"},
                    {"date": "2025-ongoing", "event": "Multiple potential buyers (Oracle, Microsoft, Perplexity AI consortium, Frank McCourt/Project Liberty); ByteDance resisting full algorithm sale", "confidence": "confirmed"},
                ],
                "project_texas": {
                    "description": "TikTok's $1.5B initiative to store US data on Oracle Cloud and limit China access",
                    "status": "Implemented but deemed insufficient by US government",
                    "oracle_partnership_value_usd": 1_500_000_000,
                    "confidence": "confirmed",
                },
                "national_security_concern": "US government asserts CCP could compel ByteDance to share US user data or manipulate algorithm for propaganda",
                "confidence": "confirmed",
            },

            "political_influence": {
                "lobbying_annual_usd": 8_700_000,
                "lobbying_confidence": "estimated",
                "key_issues": ["Preventing US ban/forced sale", "COPPA compliance", "Data privacy"],
                "cultural_influence": "TikTok shapes youth culture, music discovery, political movements; 'TikTok made me buy it' drives consumer spending",
                "election_influence": "TikTok political content reached ~50% of 18-29 year olds in 2024 election",
                "confidence": "estimated",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Chinese government influence on content moderation (suppressing Tiananmen, Uyghur content)", "year": "ongoing", "confidence": "estimated"},
                    {"issue": "Youth mental health / addiction concerns (infinite scroll, dopamine loops)", "year": "ongoing", "confidence": "confirmed"},
                    {"issue": "COPPA violation — $5.7M fine (Musical.ly era)", "year": 2019, "confidence": "confirmed"},
                    {"issue": "Potential data access by Chinese engineers despite Project Texas", "year": 2022, "confidence": "confirmed"},
                ],
                "risk_level": "critical",
            },

            "market_impact": {
                "direct_market_impact": "Not publicly traded; impacts publicly-traded competitors (META, SNAP, GOOGL)",
                "indirect_signals": [
                    "TikTok ban = massive beneficiary for META (Reels), GOOGL (Shorts), SNAP",
                    "TikTok Shop growth = headwind for AMZN, SHOP",
                    "Oracle (ORCL) could benefit from forced-sale deal",
                ],
                "confidence": "inferred",
            },

            "trading_signals": [
                {"signal": "TikTok ban legislation progress = buy META + SNAP + GOOGL", "confidence": "derived"},
                {"signal": "TikTok forced sale to Oracle = bullish ORCL", "confidence": "inferred"},
                {"signal": "TikTok Shop GMV growth = bearish signal for traditional e-commerce", "confidence": "inferred"},
                {"signal": "Any resolution (ban or sale) removes uncertainty overhang from social media sector", "confidence": "derived"},
            ],
        },

        # ── REDDIT (RDDT) ──────────────────────────────────────────────
        "RDDT": {
            "name": "Reddit, Inc.",
            "ticker": "RDDT",
            "sector": "Social Media / Forum / Data",
            "market_cap_usd": 25_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 2_000,
            "hq": "San Francisco, CA",
            "revenue_fy2024_usd": 1_300_000_000,
            "revenue_confidence": "estimated",
            "ipo_date": "2024-03-21",
            "ipo_price_usd": 34,

            "ceo": {
                "name": "Steve Huffman",
                "title": "Co-Founder & CEO",
                "reddit_username": "u/spez",
                "background": "Co-founded Reddit 2005; returned as CEO 2015",
                "total_compensation_2024_usd": 193_000_000,
                "compensation_note": "Includes large IPO-related equity grant",
                "compensation_confidence": "confirmed",
                "confidence": "confirmed",
            },

            "platform_metrics": {
                "dau": 100_000_000,
                "mau": 430_000_000,
                "dau_confidence": "confirmed",
                "active_communities": 100_000,
                "note": "Unique 'community of communities' structure; pseudonymous; long-form discussion",
            },

            "revenue_streams": {
                "advertising": {
                    "revenue_usd": 1_100_000_000,
                    "note": "Primarily performance and brand advertising; growing but CPMs below Meta/Google",
                    "confidence": "estimated",
                },
                "data_licensing": {
                    "revenue_usd": 200_000_000,
                    "note": "Licenses user-generated content to AI companies for LLM training",
                    "partners": ["Google ($60M/yr)", "OpenAI (terms undisclosed)", "Others"],
                    "confidence": "estimated",
                    "controversy": "Users argue their content is being sold without compensation; subreddit blackouts June 2023 over API pricing",
                },
            },

            "ai_data_licensing": {
                "description": "Reddit is one of the largest sources of human-generated training data for LLMs",
                "google_deal_usd_annual": 60_000_000,
                "google_deal_confidence": "confirmed",
                "strategic_value": "Reddit's data licensing is a novel revenue stream; positions company as 'data provider to AI'",
                "risk": "If AI chatbots reduce need to visit Reddit (zero-click answers), data licensing could cannibalize core business",
                "confidence": "inferred",
            },

            "political_influence": {
                "lobbying_annual_usd": 820_000,
                "lobbying_confidence": "estimated",
                "cultural_influence": "r/wallstreetbets drove GameStop short squeeze (Jan 2021); subreddits shape political movements",
                "wallstreetbets_impact": {
                    "description": "r/wallstreetbets (16M members) demonstrated retail investor power; spawned 'meme stock' phenomenon",
                    "gme_impact": "GameStop squeeze caused $20B+ in hedge fund losses (Melvin Capital liquidated)",
                    "confidence": "confirmed",
                },
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "API pricing controversy — third-party app shutdown (Apollo, etc.)", "year": 2023, "confidence": "confirmed"},
                    {"issue": "Subreddit blackout protests (June 2023)", "year": 2023, "confidence": "confirmed"},
                    {"issue": "Historical content moderation failures (jailbait, The_Donald)", "year": "historical", "confidence": "confirmed"},
                    {"issue": "Data licensing without user consent", "year": "2023-ongoing", "confidence": "confirmed"},
                ],
                "risk_level": "moderate",
            },

            "trading_signals": [
                {"signal": "Reddit DAU growth + ad revenue per user expansion = core thesis", "confidence": "derived"},
                {"signal": "AI data licensing deals = high-margin incremental revenue; watch for new partnerships", "confidence": "inferred"},
                {"signal": "r/wallstreetbets activity spikes = potential meme stock rally indicator", "confidence": "confirmed"},
                {"signal": "Post-IPO lockup expirations create selling pressure windows", "confidence": "derived"},
            ],
        },

        # ── SNAP INC (SNAP) ────────────────────────────────────────────
        "SNAP": {
            "name": "Snap Inc.",
            "ticker": "SNAP",
            "sector": "Social Media / AR / Camera",
            "market_cap_usd": 20_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 5_300,
            "hq": "Santa Monica, CA",
            "revenue_fy2024_usd": 5_400_000_000,
            "revenue_confidence": "estimated",

            "ceo": {
                "name": "Evan Spiegel",
                "title": "Co-Founder & CEO",
                "voting_control_pct": 99,
                "dual_class": True,
                "dual_class_note": "Class C shares give Spiegel and co-founder Bobby Murphy near-total voting control; most extreme dual-class structure in tech",
                "net_worth_estimated_usd": 4_500_000_000,
                "net_worth_confidence": "estimated",
                "background": "Stanford dropout; invented Snapchat (disappearing messages); married to Miranda Kerr",
                "confidence": "confirmed",
            },

            "platform_metrics": {
                "dau": 414_000_000,
                "dau_confidence": "confirmed",
                "note": "Snapchat reaches 75% of 13-34 year olds in 25+ countries",
                "snapchat_plus_subscribers": 12_000_000,
                "subscribers_confidence": "estimated",
            },

            "business_segments": {
                "advertising": {
                    "revenue_usd": 5_000_000_000,
                    "note": "Ad revenue growth recovering after 2022 trough; Snap's ad measurement + SMB tools improving",
                    "confidence": "estimated",
                },
                "snapchat_plus": {
                    "revenue_usd": 400_000_000,
                    "note": "$3.99/mo subscription; grew from zero to 12M subs in 2 years",
                    "confidence": "estimated",
                },
                "ar_enterprise": {
                    "note": "AR try-on for commerce (fashion, beauty); Snap AR Enterprise Services",
                    "spectacles_ar_glasses": "Developer-only AR glasses; not consumer product yet",
                    "confidence": "estimated",
                },
            },

            "ar_strategy": {
                "description": "Snap has positioned itself as an AR-first company; 300K+ AR creators; 300B+ AR lens plays",
                "ar_investment_cumulative_usd": 5_000_000_000,
                "ar_confidence": "estimated",
                "spectacles": "5th gen AR glasses (developer kit 2024); full consumer launch TBD",
                "competitive_position": "Leading in AR filters/lenses; but Apple Vision Pro and Meta Quest threaten long-term AR leadership",
                "confidence": "inferred",
            },

            "political_influence": {
                "lobbying_annual_usd": 1_600_000,
                "lobbying_confidence": "estimated",
                "key_issues": ["COPPA / child safety", "Section 230", "Privacy regulation"],
                "note": "Lower political profile than Meta/X; Snap has proactively engaged with child safety regulation",
                "confidence": "estimated",
            },

            "content_moderation": {
                "controversies": [
                    {"issue": "Snap Map safety concerns for minors", "year": "ongoing", "confidence": "confirmed"},
                    {"issue": "Drug dealer activity via Snapchat (fentanyl)", "year": "ongoing", "confidence": "confirmed"},
                    {"issue": "My AI chatbot safety concerns for teens", "year": 2023, "confidence": "confirmed"},
                ],
                "risk_level": "moderate",
            },

            "trading_signals": [
                {"signal": "Snap ad revenue growth rate = canary in coal mine for digital ad market (Snap reports before Meta)", "confidence": "confirmed"},
                {"signal": "DAU growth in international markets (India) = long-term bull case", "confidence": "inferred"},
                {"signal": "TikTok ban = significant beneficiary (competing for same demographic)", "confidence": "derived"},
                {"signal": "AR/Spectacles consumer launch = potential re-rating catalyst but high execution risk", "confidence": "inferred"},
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # FINANCIAL MEDIA
    # ══════════════════════════════════════════════════════════════════════

    "financial_media": {

        # ── BLOOMBERG LP ────────────────────────────────────────────────
        "BLOOMBERG": {
            "name": "Bloomberg L.P.",
            "ticker": None,
            "sector": "Financial Data / Media",
            "ownership": "private (88% Michael Bloomberg, 20% held by Bloomberg employees via partnership)",
            "hq": "New York, NY",

            "founder_owner": {
                "name": "Michael Bloomberg",
                "ownership_pct": 88,
                "net_worth_estimated_usd": 106_000_000_000,
                "net_worth_confidence": "estimated",
                "background": "Former NYC Mayor (2002-2013); 2020 presidential candidate (spent $1B of own money); Salomon Brothers bond trader turned data mogul",
                "political_donations": "Major Democratic donor; climate change, gun control advocacy; spent ~$1B on 2020 presidential campaign",
                "confidence": "confirmed",
            },

            "financials": {
                "revenue_2024_usd": 13_000_000_000,
                "revenue_confidence": "estimated",
                "revenue_source": "Company disclosure + media reports",
                "employees": 20_000,
            },

            "bloomberg_terminal": {
                "subscribers": 350_000,
                "subscribers_confidence": "estimated",
                "price_per_terminal_annual_usd": 28_800,
                "terminal_revenue_estimated_usd": 10_000_000_000,
                "terminal_confidence": "derived",
                "market_position": "Near-monopoly on professional financial data; ~33% market share of financial data market",
                "moat": "Network effects (messaging, data sharing) + 30+ years of historical data + workflow integration",
                "competitors": ["Refinitiv/LSEG (Reuters terminal)", "FactSet", "S&P Capital IQ"],
                "threat": "AI-powered alternatives (ChatGPT for finance, alternative data providers) could erode terminal lock-in long-term",
                "confidence": "estimated",
            },

            "bloomberg_news": {
                "journalists": 2_700,
                "bureaus": 120,
                "reach": "Bloomberg TV, Bloomberg Radio, Bloomberg.com, Bloomberg Businessweek",
                "market_impact": "Bloomberg headlines on terminal move markets; algo-parsed for trading signals",
                "bloomberg_terminal_news": "Terminal users see headlines first — information advantage for subscribers",
                "confidence": "estimated",
            },

            "political_influence": {
                "lobbying": "Not significant directly; Bloomberg's personal political spending is the influence vector",
                "bloomberg_personal_spending": {
                    "everytown_gun_safety": "Funded $100M+ in gun control advocacy",
                    "climate": "Beyond Carbon campaign — $500M to retire coal plants",
                    "2020_campaign": "$1B+ personal spend",
                    "confidence": "confirmed",
                },
                "editorial_independence_concern": "Bloomberg News has policy of not investigating Michael Bloomberg or his wealth; creates editorial blind spot",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Bloomberg terminal subscriber count = proxy for Wall Street headcount / health", "confidence": "inferred"},
                {"signal": "Bloomberg headline sentiment (algo-parsed) is standard institutional alpha signal", "confidence": "confirmed"},
                {"signal": "Bloomberg not publicly traded — no direct trading opportunity", "confidence": "confirmed"},
            ],
        },

        # ── CNBC / COMCAST ─────────────────────────────────────────────
        "CNBC": {
            "name": "CNBC (Comcast / NBCUniversal)",
            "ticker": "CMCSA",
            "sector": "Financial Television / Media",
            "parent": "Comcast Corporation (see CMCSA in media_conglomerates)",

            "overview": {
                "description": "Primary US business/financial news television network",
                "viewers_primetime": 200_000,
                "viewers_market_hours": 350_000,
                "viewers_confidence": "estimated",
                "digital_mau": 100_000_000,
                "digital_confidence": "estimated",
                "revenue_estimated_usd": 1_200_000_000,
                "revenue_confidence": "estimated",
            },

            "key_personalities": [
                {
                    "name": "Jim Cramer",
                    "show": "Mad Money",
                    "influence": "Inverse Cramer ETF (SJIM) created as meme; measurable short-term price impact on stock mentions",
                    "inverse_cramer_effect": "Academic studies suggest Cramer picks underperform market after 12 months; short-term retail herding effect",
                    "confidence": "estimated",
                },
                {"name": "David Faber", "show": "Squawk on the Street", "influence": "Primary M&A breaking news journalist", "confidence": "confirmed"},
                {"name": "Sara Eisen", "show": "Closing Bell", "confidence": "confirmed"},
                {"name": "Scott Wapner", "show": "Halftime Report", "confidence": "confirmed"},
                {"name": "Joe Kernen", "show": "Squawk Box", "note": "Conservative lean; interviews Fed chairs, CEOs", "confidence": "confirmed"},
            ],

            "market_impact": {
                "real_time_sentiment": "CNBC coverage drives real-time retail sentiment; Cramer effect is measurable",
                "ceo_interviews": "CEO appearances on CNBC can move stock ±2-5% in minutes",
                "breaking_news": "M&A leaks, Fed commentary often first reported on CNBC",
                "fed_coverage": "Steve Liesman is primary Fed correspondent; his interpretation of Fed statements moves bonds",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Cramer stock mention = short-term retail buying spike (1-3 days); fade after", "confidence": "estimated"},
                {"signal": "CNBC 'breaking news' M&A chyron = frontrunnable by algos parsing the feed", "confidence": "confirmed"},
                {"signal": "CNBC panic coverage (BREAKING: markets plunging) = contrarian buy signal at extremes", "confidence": "inferred"},
                {"signal": "CNBC CEO interview schedule = potential catalyst calendar", "confidence": "inferred"},
            ],
        },

        # ── REUTERS / THOMSON REUTERS ──────────────────────────────────
        "TRI": {
            "name": "Thomson Reuters Corporation",
            "ticker": "TRI",
            "sector": "Financial Data / News / Legal / Tax",
            "market_cap_usd": 85_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 26_000,
            "hq": "Toronto, Canada",
            "revenue_fy2024_usd": 7_200_000_000,
            "revenue_confidence": "confirmed",

            "controlling_shareholder": {
                "name": "Thomson family (via Woodbridge Company)",
                "ownership_pct": 69,
                "david_thomson_net_worth_usd": 75_000_000_000,
                "net_worth_confidence": "estimated",
                "note": "David Thomson is richest Canadian; inherited from father Ken Thomson; family controls via Woodbridge holding company",
                "confidence": "confirmed",
            },

            "business_segments": {
                "legal_professionals": {
                    "revenue_usd": 2_800_000_000,
                    "includes": ["Westlaw", "Practical Law", "Legal AI (CoCounsel)"],
                    "confidence": "confirmed",
                },
                "corporates": {
                    "revenue_usd": 1_700_000_000,
                    "includes": ["CLEAR (due diligence)", "Checkpoint (tax compliance)", "Confirmation.com"],
                    "confidence": "confirmed",
                },
                "tax_accounting": {
                    "revenue_usd": 1_000_000_000,
                    "confidence": "confirmed",
                },
                "reuters_news": {
                    "revenue_usd": 800_000_000,
                    "note": "Reuters employs 2,500+ journalists globally; wire service model",
                    "market_impact": "Reuters headlines are algo-parsed by HFT and institutional traders; moves markets in milliseconds",
                    "confidence": "estimated",
                },
            },

            "lseg_stake": {
                "description": "Thomson Reuters sold Refinitiv (financial data) to London Stock Exchange Group (LSEG) for $27B in 2021; retained ~15% LSEG stake",
                "lseg_stake_value_usd": 20_000_000_000,
                "note": "LSEG Refinitiv competes with Bloomberg Terminal (Eikon/Workspace)",
                "confidence": "confirmed",
            },

            "ai_strategy": {
                "description": "Heavy investment in AI for legal, tax, and news; CoCounsel (GenAI for legal research) based on GPT-4",
                "ai_revenue_target": "Expects AI to add $10B in addressable market over 5 years",
                "confidence": "estimated",
            },

            "trading_signals": [
                {"signal": "Reuters headline algo parsing is standard alpha signal; speed of parsing = edge", "confidence": "confirmed"},
                {"signal": "TRI AI revenue growth = multiple expansion driver (legal AI is high-margin)", "confidence": "inferred"},
                {"signal": "Stable, recurring revenue model makes TRI defensive in downturns", "confidence": "derived"},
            ],
        },

        # ── FINTWIT / CRYPTO TWITTER ───────────────────────────────────
        "FINTWIT": {
            "name": "Financial Twitter / Crypto Twitter (decentralized)",
            "ticker": None,
            "sector": "Decentralized Financial Commentary / Influence",
            "ownership": "decentralized — hosted on X (Twitter), YouTube, Substack",

            "overview": {
                "description": "Loosely organized network of traders, analysts, and influencers who shape real-time market sentiment",
                "platforms": ["X/Twitter", "YouTube", "Substack", "Discord", "Telegram"],
                "influence_assessment": "Disproportionate market influence relative to formal credentials; retail flow follows these voices",
                "confidence": "inferred",
            },

            "key_influencers": [
                {
                    "handle": "@CryptoCobain (Cobie)",
                    "real_name": "Jordan Fish",
                    "followers_estimated": 700_000,
                    "influence": "Crypto thought leader; co-founded UpOnly podcast; early calls on multiple tokens",
                    "track_record": "Mixed — excellent directional calls offset by some bad entries",
                    "confidence": "estimated",
                },
                {
                    "handle": "@GCRClassic (GCR)",
                    "real_name": "Unknown",
                    "influence": "Legendary crypto trader; famous for $10M+ Luna short before collapse",
                    "note": "Pseudonymous; minimal posting; massive influence when active",
                    "confidence": "estimated",
                },
                {
                    "handle": "@unusual_whales",
                    "influence": "Options flow tracking; congressional trading alerts; ~1M followers",
                    "product": "Unusual Whales terminal — retail options flow analytics",
                    "confidence": "confirmed",
                },
                {
                    "handle": "@DeItaone (Walter Bloomberg)",
                    "influence": "Fastest Bloomberg terminal headline reposter on X; algos parse his feed",
                    "note": "Not affiliated with Bloomberg LP; just fast at reposting terminal headlines",
                    "confidence": "confirmed",
                },
                {
                    "handle": "@zaborowen / Zerohedge",
                    "influence": "Bearish macro commentary; conspiracy-adjacent; large reach (~2M followers)",
                    "note": "Banned from X, reinstated; registered as Bulgarian nationals under pseudonym 'Tyler Durden'",
                    "confidence": "confirmed",
                },
            ],

            "market_impact": {
                "retail_flow_influence": "FinTwit/CT sentiment shifts precede retail broker flow by 1-4 hours",
                "meme_stock_coordination": "GameStop (GME), AMC, BBBY rallies partly coordinated via FinTwit + Reddit",
                "crypto_market_impact": "CT influencer calls move small-cap tokens ±20-50% within hours",
                "sentiment_as_signal": "Aggregate FinTwit sentiment is tradeable alpha signal (contrarian at extremes)",
                "confidence": "inferred",
            },

            "trading_signals": [
                {"signal": "FinTwit consensus bearishness = contrarian buy signal (and vice versa)", "confidence": "inferred"},
                {"signal": "CT influencer token mentions = front-runnable pump signal (but also dump risk)", "confidence": "estimated"},
                {"signal": "@DeItaone (Walter Bloomberg) headline speed = tradeable edge for retail", "confidence": "confirmed"},
                {"signal": "Unusual Whales congressional trade alerts = 2-5 day anticipatory signal", "confidence": "estimated"},
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # ADVERTISING & DATA COMPLEX
    # ══════════════════════════════════════════════════════════════════════

    "advertising_data": {

        # ── GOOGLE ADS (GOOGL) ─────────────────────────────────────────
        "GOOGL_ADS": {
            "name": "Google Advertising (Alphabet Inc.)",
            "ticker": "GOOGL",
            "sector": "Digital Advertising / Search / Ad-Tech",
            "parent_market_cap_usd": 2_200_000_000_000,
            "market_cap_confidence": "estimated",

            "ad_revenue": {
                "total_2024_usd": 264_600_000_000,
                "total_confidence": "confirmed",
                "total_source": "Alphabet 10-K FY2024",
                "breakdown": {
                    "google_search_usd": 191_000_000_000,
                    "youtube_ads_usd": 36_800_000_000,
                    "google_network_usd": 30_400_000_000,
                    "other_usd": 6_400_000_000,
                },
                "global_digital_ad_market_share_pct": 39,
                "share_confidence": "estimated",
            },

            "doj_antitrust": {
                "case_1_search": {
                    "status": "DOJ won — Judge Mehta ruled Google is illegal monopoly in search (Aug 2024)",
                    "remedy_phase": "Ongoing — DOJ proposed forcing Google to divest Chrome browser and potentially Android",
                    "chrome_divestiture_risk": "If forced to sell Chrome, destroys Google's search distribution moat",
                    "potential_impact": "Most significant US antitrust ruling since Microsoft (2001)",
                    "confidence": "confirmed",
                },
                "case_2_adtech": {
                    "status": "Trial completed Dec 2024; ruling expected 2025",
                    "allegation": "Google monopolized ad-tech stack (buy-side DV360, sell-side AdX, exchange Ad Manager)",
                    "potential_remedy": "Forced divestiture of ad-tech components",
                    "confidence": "confirmed",
                },
            },

            "political_influence": {
                "note": "See GOOGL in tech_monopoly_network.py for full political profile",
                "lobbying_annual_usd": 13_200_000,
                "lobbying_confidence": "estimated",
                "key_issues": ["Antitrust defense", "AI regulation", "Privacy regulation", "Section 230"],
            },

            "trading_signals": [
                {"signal": "Google ad revenue growth = best proxy for overall digital ad market health", "confidence": "confirmed"},
                {"signal": "DOJ remedy ruling (Chrome divestiture) = massive binary event risk for GOOGL", "confidence": "confirmed"},
                {"signal": "AI Overviews reducing click-through rates = structural threat to search ad model", "confidence": "inferred"},
                {"signal": "Google Network revenue decline = canary for programmatic ad market weakness", "confidence": "derived"},
            ],
        },

        # ── META ADS ───────────────────────────────────────────────────
        "META_ADS": {
            "name": "Meta Advertising (Meta Platforms, Inc.)",
            "ticker": "META",
            "sector": "Digital Advertising / Social",
            "note": "See META in social_media section for full company profile",

            "ad_revenue": {
                "total_2024_usd": 160_000_000_000,
                "total_confidence": "confirmed",
                "breakdown": {
                    "facebook_usd": 70_000_000_000,
                    "instagram_usd": 65_000_000_000,
                    "messenger_whatsapp_usd": 15_000_000_000,
                    "audience_network_usd": 10_000_000_000,
                },
                "breakdown_confidence": "estimated",
                "breakdown_note": "Meta does not break out platform-level ad revenue; estimates from eMarketer/analysts",
                "global_digital_ad_market_share_pct": 22,
                "share_confidence": "estimated",
            },

            "privacy_regulation_impact": {
                "apple_att_impact": {
                    "description": "Apple App Tracking Transparency (iOS 14.5, April 2021) cost Meta ~$10B in 2022 ad revenue",
                    "recovery": "Meta rebuilt ad targeting using AI/ML + on-platform data; largely recovered by 2024",
                    "confidence": "confirmed",
                },
                "eu_gdpr_fines": [
                    {"amount_eur": 1_200_000_000, "date": "2023-05", "reason": "EU-US data transfers (Schrems II)", "confidence": "confirmed"},
                    {"amount_eur": 390_000_000, "date": "2023-01", "reason": "Legal basis for behavioral advertising", "confidence": "confirmed"},
                ],
                "us_privacy_risk": "State-level privacy laws (California CPRA, etc.) + potential federal privacy bill could constrain targeting",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Meta ARPU (average revenue per user) growth = best indicator of ad pricing power", "confidence": "derived"},
                {"signal": "Apple privacy changes are now priced in; further iOS restrictions are diminishing marginal impact", "confidence": "inferred"},
                {"signal": "Click-to-WhatsApp/Messenger ads = fastest-growing format; watch for acceleration", "confidence": "inferred"},
            ],
        },

        # ── THE TRADE DESK (TTD) ───────────────────────────────────────
        "TTD": {
            "name": "The Trade Desk, Inc.",
            "ticker": "TTD",
            "sector": "Programmatic Advertising / Ad-Tech",
            "market_cap_usd": 55_000_000_000,
            "market_cap_confidence": "estimated",
            "employees": 3_500,
            "hq": "Ventura, CA",
            "revenue_fy2024_usd": 2_400_000_000,
            "revenue_confidence": "confirmed",

            "ceo": {
                "name": "Jeff Green",
                "title": "Co-Founder & CEO",
                "net_worth_estimated_usd": 8_000_000_000,
                "net_worth_confidence": "estimated",
                "background": "Built TTD as independent demand-side platform (DSP); avoids owning media to maintain neutrality",
                "confidence": "confirmed",
            },

            "business_model": {
                "description": "Independent demand-side platform (DSP) — helps advertisers buy digital ads programmatically across CTV, display, mobile, audio",
                "key_advantage": "Only major DSP not owned by a media company; advertisers trust TTD's objectivity",
                "take_rate_pct": 20,
                "take_rate_confidence": "estimated",
                "ctv_growth": "Connected TV (CTV) is fastest-growing segment; TTD benefits from shift from linear TV to streaming ads",
                "uid2": "Unified ID 2.0 — TTD's open-source identity framework to replace third-party cookies",
            },

            "competitive_position": {
                "vs_google_dv360": "TTD positioned as Google alternative; benefits from DOJ ad-tech case forcing advertisers to diversify",
                "vs_amazon_dsp": "Amazon DSP growing but limited to Amazon ecosystem; TTD is platform-agnostic",
                "moat": "Advertiser relationships + data integrations + CTV inventory access + UID2 adoption",
                "confidence": "inferred",
            },

            "trading_signals": [
                {"signal": "TTD revenue growth rate = pure-play indicator of programmatic ad market health", "confidence": "derived"},
                {"signal": "CTV ad spend growth benefits TTD disproportionately vs. walled gardens", "confidence": "inferred"},
                {"signal": "DOJ Google ad-tech ruling = potential massive catalyst for TTD (advertisers forced to diversify)", "confidence": "inferred"},
                {"signal": "TTD trades at 50x+ revenue — high valuation means ANY deceleration causes sharp selloff", "confidence": "derived"},
            ],
        },

        # ── DATA BROKERS / SURVEILLANCE ────────────────────────────────
        "DATA_BROKERS": {
            "name": "Data Broker & Surveillance Complex",
            "ticker": None,
            "sector": "Data Brokerage / Surveillance / Analytics",

            "palantir": {
                "ticker": "PLTR",
                "name": "Palantir Technologies",
                "market_cap_usd": 160_000_000_000,
                "market_cap_confidence": "estimated",
                "revenue_2024_usd": 2_900_000_000,
                "revenue_confidence": "confirmed",
                "ceo": {
                    "name": "Alex Karp",
                    "title": "Co-Founder & CEO",
                    "background": "Stanford Law + Frankfurt PhD; eccentric; close to intelligence community",
                    "total_compensation_2024_usd": 7_400_000,
                    "confidence": "confirmed",
                },
                "co_founder": {
                    "name": "Peter Thiel",
                    "role": "Co-Founder & Board member",
                    "net_worth_estimated_usd": 11_000_000_000,
                    "net_worth_confidence": "estimated",
                    "note": "PayPal co-founder; Founders Fund; major Republican donor; close to Trump administration; surveillance state architect",
                    "confidence": "confirmed",
                },
                "government_contracts": {
                    "pct_government_revenue": 55,
                    "clients": ["CIA", "NSA", "FBI", "ICE", "US Army", "UK NHS", "Ukraine military"],
                    "controversy": "ICE contract used to track undocumented immigrants; employee protests",
                    "confidence": "confirmed",
                },
                "commercial_growth": {
                    "description": "AIP (Artificial Intelligence Platform) driving commercial adoption; bootcamps convert enterprises",
                    "commercial_revenue_growth_pct": 40,
                    "confidence": "estimated",
                },
                "trading_signals": [
                    {"signal": "PLTR government contract wins (especially defense/intelligence) = revenue catalyst", "confidence": "derived"},
                    {"signal": "AIP commercial adoption rate = key growth driver; watch quarterly commercial customer count", "confidence": "inferred"},
                    {"signal": "PLTR trades at extreme valuation (60x+ revenue) — momentum stock, not value", "confidence": "derived"},
                ],
            },

            "clearview_ai": {
                "name": "Clearview AI",
                "ticker": None,
                "ownership": "private",
                "ceo": {"name": "Hoan Ton-That", "title": "Co-Founder & CEO", "confidence": "confirmed"},
                "description": "Facial recognition company; scraped 40B+ photos from internet without consent",
                "clients": ["600+ US law enforcement agencies", "DHS", "FBI"],
                "legal_battles": [
                    {"jurisdiction": "Illinois", "case": "BIPA class action — $52M settlement (2024)", "confidence": "confirmed"},
                    {"jurisdiction": "EU", "case": "Multiple GDPR fines (Italy €20M, Greece €20M, France €20M, UK £7.5M)", "confidence": "confirmed"},
                    {"jurisdiction": "Australia", "case": "Found to violate Privacy Act", "confidence": "confirmed"},
                    {"jurisdiction": "Canada", "case": "Found to violate PIPEDA; ordered to delete Canadian data", "confidence": "confirmed"},
                ],
                "privacy_implications": "Clearview AI represents the extreme end of surveillance capitalism — biometric data harvested at scale without consent",
                "confidence": "confirmed",
            },

            "data_broker_ecosystem": {
                "description": "Multi-billion dollar industry selling personal data (location, browsing, purchase history, health)",
                "major_players": [
                    {"name": "Acxiom (now LiveRamp)", "ticker": "RAMP", "specialty": "Identity resolution, data onboarding"},
                    {"name": "Oracle Data Cloud", "ticker": "ORCL", "specialty": "Shutting down third-party ad data business (2024)", "note": "Oracle exited data brokerage amid privacy concerns"},
                    {"name": "Experian", "ticker": "EXPN.L", "specialty": "Credit data + marketing data"},
                    {"name": "TransUnion", "ticker": "TRU", "specialty": "Credit data + identity verification"},
                    {"name": "LexisNexis Risk Solutions", "parent": "RELX (RELX)", "specialty": "Legal, identity, insurance data"},
                    {"name": "Verisk", "ticker": "VRSK", "specialty": "Insurance data analytics"},
                ],
                "regulatory_risk": "FTC increasingly targeting data brokers; CFPB proposed rules to regulate data industry; state privacy laws expanding",
                "confidence": "confirmed",
            },

            "trading_signals": [
                {"signal": "Privacy regulation expansion = headwind for data brokers, tailwind for privacy-focused ad-tech (TTD, Apple)", "confidence": "inferred"},
                {"signal": "PLTR commercial growth acceleration vs. government concentration risk = key tension", "confidence": "derived"},
                {"signal": "Data broker regulatory crackdown = potential catalyst for LiveRamp (RAMP) as compliant identity solution", "confidence": "inferred"},
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # CROSS-SECTOR DYNAMICS
    # ══════════════════════════════════════════════════════════════════════

    "cross_sector_dynamics": {
        "cord_cutting": {
            "description": "Linear TV subscribers declining 6-8% annually; accelerating since 2020",
            "pay_tv_subs_2024": 62_000_000,
            "pay_tv_subs_2019": 86_000_000,
            "beneficiaries": ["NFLX", "GOOGL (YouTube TV)", "AMZN (Prime Video)", "SPOT"],
            "losers": ["CMCSA", "CHTR", "DIS (ESPN linear)", "FOXA (Fox News)", "PARA"],
            "confidence": "confirmed",
        },

        "sports_rights_arms_race": {
            "description": "Live sports is the last moat for linear TV; streaming platforms now competing aggressively for rights",
            "major_deals_2024_2025": [
                {"league": "NFL", "buyers": ["ESPN/Disney", "Fox", "CBS/Paramount", "Amazon (TNF)", "Netflix (Christmas)"], "total_annual_usd": 12_000_000_000, "confidence": "confirmed"},
                {"league": "NBA", "buyers": ["ESPN/Disney", "Amazon", "NBC/Comcast"], "total_11yr_usd": 76_000_000_000, "note": "Warner Bros Discovery lost TNT's NBA rights", "confidence": "confirmed"},
                {"league": "MLB", "buyers": ["Fox", "ESPN", "TBS (Warner)", "Apple TV+"], "confidence": "confirmed"},
                {"league": "MLS", "buyers": ["Apple TV+ ($2.5B/10yr exclusive)"], "confidence": "confirmed"},
                {"league": "WWE", "buyers": ["Netflix ($5B/10yr for Raw)"], "confidence": "confirmed"},
            ],
            "trading_signal": "Sports rights costs are escalating faster than ad revenue growth — margin compression for media companies",
            "signal_confidence": "derived",
        },

        "ai_disruption_of_media": {
            "description": "Generative AI threatens content creation, journalism, advertising, and information discovery",
            "threats": [
                {"area": "Journalism", "threat": "AI-generated news articles reducing demand for human journalists", "confidence": "confirmed"},
                {"area": "Advertising", "threat": "AI-generated ad creative reduces agency spend", "confidence": "inferred"},
                {"area": "Search/Discovery", "threat": "AI answers reduce need to click through to publishers (zero-click problem)", "confidence": "confirmed"},
                {"area": "Music", "threat": "AI-generated music flooding Spotify/platforms; copyright unclear", "confidence": "confirmed"},
                {"area": "Film/TV", "threat": "AI used for VFX, scriptwriting, dubbing; 2023 SAG-AFTRA/WGA strikes partly over AI", "confidence": "confirmed"},
            ],
            "beneficiaries": ["GOOGL (Gemini)", "META (Llama)", "MSFT (Copilot)", "AMZN (Bedrock)"],
            "losers": ["Traditional publishers", "Ad agencies (WPP, Omnicom, Publicis)", "Stock photo (Getty)"],
            "confidence": "inferred",
        },

        "information_warfare": {
            "description": "State actors and private entities use media platforms for information operations",
            "vectors": [
                {"actor": "Russia", "methods": ["RT (banned in EU)", "Troll farms (IRA)", "Amplification bots on X/Facebook"], "confidence": "confirmed"},
                {"actor": "China", "methods": ["TikTok algorithm concerns", "WeChat censorship", "State media amplification"], "confidence": "estimated"},
                {"actor": "Iran", "methods": ["Social media impersonation campaigns", "PressTV"], "confidence": "estimated"},
                {"actor": "US domestic", "methods": ["PAC-funded 'news' sites", "Astroturfing campaigns", "Algorithmic amplification of outrage"], "confidence": "confirmed"},
            ],
            "market_implications": "Information warfare creates volatility; false narratives can move markets before correction",
            "confidence": "confirmed",
        },

        "ownership_concentration": {
            "description": "6 companies control ~90% of US media consumed by the public",
            "the_six": ["Disney (DIS)", "Comcast (CMCSA)", "Warner Bros Discovery (WBD)", "Paramount (PARA)", "Fox (FOXA)", "Sony (SONY)"],
            "notable_independents": ["Netflix (NFLX)", "Spotify (SPOT)", "Reddit (RDDT)", "Snap (SNAP)"],
            "tech_platform_gatekeepers": ["Alphabet/Google (GOOGL)", "Meta (META)", "Apple (AAPL)", "Amazon (AMZN)", "ByteDance (TikTok)"],
            "antitrust_risk": "Increasing regulatory scrutiny of media consolidation + tech platform power",
            "confidence": "confirmed",
        },
    },
}
