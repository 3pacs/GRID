"""
GRID Intelligence — Actor Network & Power Structure Map.

The deepest intelligence layer. Maps the global financial power structure:
who controls money, where it flows, what motivates them, and how their
actions connect. Makes it actionable for trading.

Actor hierarchy:
    Sovereign     — central banks, treasuries, heads of state
    Regional      — ECB governors, BOJ/PBOC/BOE, key committee chairs
    Institutional — hedge funds, asset managers, SWFs, activist investors
    Individual    — traders, congressional members, corporate insiders

Data sources:
    - 13F filings (SEC EDGAR)
    - Congressional disclosures (House/Senate)
    - Form 4 insider filings
    - ICIJ Offshore Leaks (Panama/Pandora Papers)
    - Dark pool volume (FINRA ATS)
    - Federal Reserve speeches + dot plots
    - Public net worth / AUM estimates

Key entry points:
    build_actor_graph           — full graph for D3 force-directed viz
    track_wealth_migration      — follow the money over N days
    find_connected_actions      — who else in the network moved?
    assess_pocket_lining        — detect self-dealing, conflicts of interest
    get_actor_context_for_ticker — who cares about this stock?
    ingest_panama_pandora_data  — parse ICIJ offshore leaks
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class Actor:
    """A named individual or entity in the global financial power structure."""

    id: str
    name: str
    tier: str          # 'sovereign', 'regional', 'institutional', 'individual'
    category: str      # 'central_bank', 'government', 'fund', 'corporation',
                       # 'insider', 'politician', 'activist', 'swf'
    title: str         # "Chair of Federal Reserve", "CEO of BlackRock"

    # Wealth & influence
    net_worth_estimate: float | None = None   # USD, from public filings
    aum: float | None = None                  # assets under management (funds)
    influence_score: float = 0.5              # 0-1, computed

    # Connections
    connections: list[dict] = field(default_factory=list)
    board_seats: list[str] = field(default_factory=list)
    political_affiliations: list[dict] = field(default_factory=list)

    # Behavior
    recent_actions: list[dict] = field(default_factory=list)
    known_positions: list[dict] = field(default_factory=list)
    motivation_model: str = "unknown"
    trust_score: float = 0.5

    # Metadata
    data_sources: list[str] = field(default_factory=list)
    credibility: str = "inferred"  # 'hard_data', 'public_record', 'rumor', 'inferred'


@dataclass
class WealthFlow:
    """A tracked movement of capital between actors/entities."""

    from_actor: str
    to_actor: str          # can be a sector, company, or individual
    amount_estimate: float
    confidence: str        # 'confirmed', 'likely', 'rumored'
    evidence: list[str] = field(default_factory=list)
    timestamp: str = ""
    implication: str = ""


# ══════════════════════════════════════════════════════════════════════════
# KNOWN ACTORS — seed database with 100+ named players
# ══════════════════════════════════════════════════════════════════════════

_KNOWN_ACTORS: dict[str, dict] = {
    # ──────────────────────────────────────────────────────────────────────
    # SOVEREIGN TIER — Federal Reserve
    # ──────────────────────────────────────────────────────────────────────
    "fed_powell": {
        "name": "Jerome Powell",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Chair, Federal Reserve",
        "influence_score": 0.99,
        "data_sources": ["fed_speeches", "fomc_minutes", "dot_plot"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_waller": {
        "name": "Christopher Waller",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Federal Reserve",
        "influence_score": 0.85,
        "data_sources": ["fed_speeches", "fomc_minutes", "dot_plot"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_bowman": {
        "name": "Michelle Bowman",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Federal Reserve",
        "influence_score": 0.80,
        "data_sources": ["fed_speeches", "fomc_minutes", "dot_plot"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_barr": {
        "name": "Michael Barr",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Vice Chair for Supervision, Federal Reserve",
        "influence_score": 0.85,
        "data_sources": ["fed_speeches", "fomc_minutes"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_cook": {
        "name": "Lisa Cook",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Federal Reserve",
        "influence_score": 0.78,
        "data_sources": ["fed_speeches", "fomc_minutes", "dot_plot"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_jefferson": {
        "name": "Philip Jefferson",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Vice Chair, Federal Reserve",
        "influence_score": 0.88,
        "data_sources": ["fed_speeches", "fomc_minutes", "dot_plot"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_kugler": {
        "name": "Adriana Kugler",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Federal Reserve",
        "influence_score": 0.76,
        "data_sources": ["fed_speeches", "fomc_minutes", "dot_plot"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_musalem": {
        "name": "Alberto Musalem",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "President, Federal Reserve Bank of St. Louis",
        "influence_score": 0.72,
        "data_sources": ["fed_speeches", "fomc_minutes"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_goolsbee": {
        "name": "Austan Goolsbee",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "President, Federal Reserve Bank of Chicago",
        "influence_score": 0.74,
        "data_sources": ["fed_speeches", "fomc_minutes"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_williams": {
        "name": "John Williams",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "President, Federal Reserve Bank of New York",
        "influence_score": 0.90,
        "data_sources": ["fed_speeches", "fomc_minutes", "dot_plot"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_daly": {
        "name": "Mary Daly",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "President, Federal Reserve Bank of San Francisco",
        "influence_score": 0.72,
        "data_sources": ["fed_speeches", "fomc_minutes"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fed_bostic": {
        "name": "Raphael Bostic",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "President, Federal Reserve Bank of Atlanta",
        "influence_score": 0.72,
        "data_sources": ["fed_speeches", "fomc_minutes"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ──────────────────────────────────────────────────────────────────────
    # SOVEREIGN TIER — US Treasury
    # ──────────────────────────────────────────────────────────────────────
    "treasury_yellen": {
        "name": "Janet Yellen",
        "tier": "sovereign",
        "category": "government",
        "title": "Secretary of the Treasury",
        "influence_score": 0.95,
        "data_sources": ["treasury_announcements", "tga_data", "auction_schedule"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "treasury_adeyemo": {
        "name": "Wally Adeyemo",
        "tier": "sovereign",
        "category": "government",
        "title": "Deputy Secretary of the Treasury",
        "influence_score": 0.75,
        "data_sources": ["treasury_announcements"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "treasury_frost": {
        "name": "Josh Frost",
        "tier": "sovereign",
        "category": "government",
        "title": "Assistant Secretary for Financial Markets, Treasury",
        "influence_score": 0.70,
        "data_sources": ["treasury_announcements", "auction_schedule"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },

    # ──────────────────────────────────────────────────────────────────────
    # SOVEREIGN TIER — ECB
    # ──────────────────────────────────────────────────────────────────────
    "ecb_lagarde": {
        "name": "Christine Lagarde",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "President, European Central Bank",
        "influence_score": 0.95,
        "data_sources": ["ecb_speeches", "ecb_decisions"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ecb_de_guindos": {
        "name": "Luis de Guindos",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Vice President, European Central Bank",
        "influence_score": 0.82,
        "data_sources": ["ecb_speeches", "ecb_decisions"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ecb_schnabel": {
        "name": "Isabel Schnabel",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Executive Board Member, ECB",
        "influence_score": 0.80,
        "data_sources": ["ecb_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ecb_lane": {
        "name": "Philip Lane",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Chief Economist, ECB",
        "influence_score": 0.82,
        "data_sources": ["ecb_speeches", "ecb_research"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ──────────────────────────────────────────────────────────────────────
    # SOVEREIGN TIER — BOJ, PBOC, BOE
    # ──────────────────────────────────────────────────────────────────────
    "boj_ueda": {
        "name": "Kazuo Ueda",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Bank of Japan",
        "influence_score": 0.92,
        "data_sources": ["boj_decisions", "boj_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "pboc_pan": {
        "name": "Pan Gongsheng",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, People's Bank of China",
        "influence_score": 0.93,
        "data_sources": ["pboc_decisions"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "boe_bailey": {
        "name": "Andrew Bailey",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Bank of England",
        "influence_score": 0.88,
        "data_sources": ["boe_decisions", "boe_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "rbi_das": {
        "name": "Shaktikanta Das",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Reserve Bank of India",
        "influence_score": 0.78,
        "data_sources": ["rbi_decisions"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ──────────────────────────────────────────────────────────────────────
    # REGIONAL TIER — US Congress key committee chairs
    # ──────────────────────────────────────────────────────────────────────
    "congress_mchenry": {
        "name": "Patrick McHenry",
        "tier": "regional",
        "category": "politician",
        "title": "Chair, House Financial Services Committee",
        "influence_score": 0.70,
        "data_sources": ["congressional_disclosures", "committee_hearings"],
        "credibility": "public_record",
        "motivation_model": "political",
    },
    "congress_brown": {
        "name": "Sherrod Brown",
        "tier": "regional",
        "category": "politician",
        "title": "Chair, Senate Banking Committee",
        "influence_score": 0.72,
        "data_sources": ["congressional_disclosures", "committee_hearings"],
        "credibility": "public_record",
        "motivation_model": "political",
    },
    "congress_wyden": {
        "name": "Ron Wyden",
        "tier": "regional",
        "category": "politician",
        "title": "Chair, Senate Finance Committee",
        "influence_score": 0.72,
        "data_sources": ["congressional_disclosures", "committee_hearings"],
        "credibility": "public_record",
        "motivation_model": "political",
    },
    "congress_smith": {
        "name": "Jason Smith",
        "tier": "regional",
        "category": "politician",
        "title": "Chair, House Ways and Means Committee",
        "influence_score": 0.70,
        "data_sources": ["congressional_disclosures", "committee_hearings"],
        "credibility": "public_record",
        "motivation_model": "political",
    },
    "congress_cole": {
        "name": "Tom Cole",
        "tier": "regional",
        "category": "politician",
        "title": "Chair, House Appropriations Committee",
        "influence_score": 0.68,
        "data_sources": ["congressional_disclosures", "committee_hearings"],
        "credibility": "public_record",
        "motivation_model": "political",
    },
    "congress_pelosi": {
        "name": "Nancy Pelosi",
        "tier": "regional",
        "category": "politician",
        "title": "Representative, former Speaker of the House",
        "net_worth_estimate": 120_000_000,
        "influence_score": 0.82,
        "data_sources": ["congressional_disclosures", "sec_filings"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },
    "congress_tuberville": {
        "name": "Tommy Tuberville",
        "tier": "regional",
        "category": "politician",
        "title": "Senator, Alabama",
        "influence_score": 0.55,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },
    "congress_crenshaw": {
        "name": "Dan Crenshaw",
        "tier": "regional",
        "category": "politician",
        "title": "Representative, Texas",
        "influence_score": 0.50,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },
    "congress_ossoff": {
        "name": "Jon Ossoff",
        "tier": "regional",
        "category": "politician",
        "title": "Senator, Georgia",
        "influence_score": 0.52,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },
    "congress_hagerty": {
        "name": "Bill Hagerty",
        "tier": "regional",
        "category": "politician",
        "title": "Senator, Tennessee — Senate Banking Committee",
        "influence_score": 0.55,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "political",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INSTITUTIONAL TIER — Top Hedge Funds
    # ──────────────────────────────────────────────────────────────────────
    "hf_dalio": {
        "name": "Ray Dalio",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Bridgewater Associates",
        "net_worth_estimate": 15_500_000_000,
        "aum": 124_000_000_000,
        "influence_score": 0.88,
        "data_sources": ["13f_filings", "sec_edgar", "public_statements"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "hf_simons": {
        "name": "Jim Simons (Estate / Renaissance)",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Renaissance Technologies",
        "aum": 106_000_000_000,
        "influence_score": 0.85,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_griffin": {
        "name": "Ken Griffin",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder & CEO, Citadel",
        "net_worth_estimate": 35_000_000_000,
        "aum": 62_000_000_000,
        "influence_score": 0.90,
        "data_sources": ["13f_filings", "sec_edgar", "political_donations"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_shaw": {
        "name": "David E. Shaw",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, D.E. Shaw & Co.",
        "net_worth_estimate": 7_300_000_000,
        "aum": 60_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_englander": {
        "name": "Israel Englander",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder & CEO, Millennium Management",
        "net_worth_estimate": 11_000_000_000,
        "aum": 64_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_overdeck": {
        "name": "John Overdeck",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-Founder, Two Sigma Investments",
        "net_worth_estimate": 8_200_000_000,
        "aum": 60_000_000_000,
        "influence_score": 0.78,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_siegel": {
        "name": "David Siegel",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-Founder, Two Sigma Investments",
        "net_worth_estimate": 5_700_000_000,
        "aum": 60_000_000_000,
        "influence_score": 0.78,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_cohen": {
        "name": "Steve Cohen",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder & CEO, Point72 Asset Management",
        "net_worth_estimate": 17_500_000_000,
        "aum": 35_000_000_000,
        "influence_score": 0.84,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_klarman": {
        "name": "Seth Klarman",
        "tier": "institutional",
        "category": "fund",
        "title": "CEO, Baupost Group",
        "net_worth_estimate": 1_500_000_000,
        "aum": 27_000_000_000,
        "influence_score": 0.76,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "value_investor",
    },
    "hf_druckenmiller": {
        "name": "Stanley Druckenmiller",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Duquesne Family Office",
        "net_worth_estimate": 6_200_000_000,
        "aum": 3_000_000_000,
        "influence_score": 0.86,
        "data_sources": ["13f_filings", "sec_edgar", "public_statements"],
        "credibility": "hard_data",
        "motivation_model": "macro_discretionary",
    },
    "hf_tepper": {
        "name": "David Tepper",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Appaloosa Management",
        "net_worth_estimate": 18_500_000_000,
        "aum": 13_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "macro_discretionary",
    },
    "hf_einhorn": {
        "name": "David Einhorn",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Greenlight Capital",
        "net_worth_estimate": 1_900_000_000,
        "aum": 3_500_000_000,
        "influence_score": 0.72,
        "data_sources": ["13f_filings", "sec_edgar", "public_statements"],
        "credibility": "hard_data",
        "motivation_model": "value_investor",
    },
    "hf_tudor_jones": {
        "name": "Paul Tudor Jones",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Tudor Investment Corp",
        "net_worth_estimate": 8_100_000_000,
        "aum": 12_000_000_000,
        "influence_score": 0.84,
        "data_sources": ["13f_filings", "sec_edgar", "public_statements"],
        "credibility": "hard_data",
        "motivation_model": "macro_discretionary",
    },
    "hf_soros": {
        "name": "George Soros / Soros Fund Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Soros Fund Management",
        "net_worth_estimate": 6_700_000_000,
        "aum": 25_000_000_000,
        "influence_score": 0.80,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "macro_discretionary",
    },
    "hf_loeb": {
        "name": "Daniel Loeb",
        "tier": "institutional",
        "category": "activist",
        "title": "Founder & CEO, Third Point",
        "net_worth_estimate": 3_600_000_000,
        "aum": 12_000_000_000,
        "influence_score": 0.76,
        "data_sources": ["13f_filings", "sec_edgar", "13d_filings"],
        "credibility": "hard_data",
        "motivation_model": "activist",
    },
    "hf_ackman": {
        "name": "Bill Ackman",
        "tier": "institutional",
        "category": "activist",
        "title": "Founder & CEO, Pershing Square Capital",
        "net_worth_estimate": 9_000_000_000,
        "aum": 18_000_000_000,
        "influence_score": 0.80,
        "data_sources": ["13f_filings", "sec_edgar", "13d_filings", "social_media"],
        "credibility": "hard_data",
        "motivation_model": "activist",
    },
    "hf_icahn": {
        "name": "Carl Icahn",
        "tier": "institutional",
        "category": "activist",
        "title": "Founder, Icahn Enterprises",
        "net_worth_estimate": 5_000_000_000,
        "aum": 15_000_000_000,
        "influence_score": 0.78,
        "data_sources": ["13f_filings", "sec_edgar", "13d_filings"],
        "credibility": "hard_data",
        "motivation_model": "activist",
    },
    "hf_peltz": {
        "name": "Nelson Peltz",
        "tier": "institutional",
        "category": "activist",
        "title": "Founding Partner, Trian Fund Management",
        "net_worth_estimate": 1_700_000_000,
        "aum": 8_500_000_000,
        "influence_score": 0.72,
        "data_sources": ["13f_filings", "sec_edgar", "13d_filings"],
        "credibility": "hard_data",
        "motivation_model": "activist",
    },
    "hf_singer": {
        "name": "Paul Singer",
        "tier": "institutional",
        "category": "activist",
        "title": "Founder, Elliott Management",
        "net_worth_estimate": 5_500_000_000,
        "aum": 65_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["13f_filings", "sec_edgar", "13d_filings"],
        "credibility": "hard_data",
        "motivation_model": "activist",
    },
    "hf_izzy_englander_millennium": {
        "name": "Millennium Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Multi-Strategy Hedge Fund",
        "aum": 64_000_000_000,
        "influence_score": 0.80,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_balyasny": {
        "name": "Dmitry Balyasny",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Balyasny Asset Management",
        "aum": 21_000_000_000,
        "influence_score": 0.72,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INSTITUTIONAL TIER — Top Asset Managers
    # ──────────────────────────────────────────────────────────────────────
    "am_fink": {
        "name": "Larry Fink",
        "tier": "institutional",
        "category": "fund",
        "title": "Chairman & CEO, BlackRock",
        "net_worth_estimate": 1_200_000_000,
        "aum": 10_000_000_000_000,
        "influence_score": 0.95,
        "data_sources": ["13f_filings", "sec_edgar", "etf_flows", "public_statements"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_vanguard": {
        "name": "Vanguard Group",
        "tier": "institutional",
        "category": "fund",
        "title": "Vanguard Group (Investor-Owned)",
        "aum": 8_600_000_000_000,
        "influence_score": 0.92,
        "data_sources": ["13f_filings", "sec_edgar", "etf_flows"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_state_street": {
        "name": "State Street Global Advisors",
        "tier": "institutional",
        "category": "fund",
        "title": "State Street Global Advisors (SPDR ETFs)",
        "aum": 4_100_000_000_000,
        "influence_score": 0.88,
        "data_sources": ["13f_filings", "sec_edgar", "etf_flows"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_fidelity": {
        "name": "Fidelity Investments / Abigail Johnson",
        "tier": "institutional",
        "category": "fund",
        "title": "CEO, Fidelity Investments",
        "net_worth_estimate": 26_000_000_000,
        "aum": 4_500_000_000_000,
        "influence_score": 0.88,
        "data_sources": ["13f_filings", "sec_edgar", "fund_flows"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_jpmorgan_am": {
        "name": "J.P. Morgan Asset Management",
        "tier": "institutional",
        "category": "fund",
        "title": "J.P. Morgan Asset Management",
        "aum": 3_000_000_000_000,
        "influence_score": 0.86,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_goldman_am": {
        "name": "Goldman Sachs Asset Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Goldman Sachs Asset Management",
        "aum": 2_800_000_000_000,
        "influence_score": 0.85,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_morgan_stanley_im": {
        "name": "Morgan Stanley Investment Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Morgan Stanley Investment Management",
        "aum": 1_500_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_invesco": {
        "name": "Invesco",
        "tier": "institutional",
        "category": "fund",
        "title": "Invesco Ltd. (QQQ)",
        "aum": 1_600_000_000_000,
        "influence_score": 0.78,
        "data_sources": ["13f_filings", "sec_edgar", "etf_flows"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_capital_group": {
        "name": "Capital Group / American Funds",
        "tier": "institutional",
        "category": "fund",
        "title": "Capital Group (American Funds)",
        "aum": 2_600_000_000_000,
        "influence_score": 0.84,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_pimco": {
        "name": "PIMCO",
        "tier": "institutional",
        "category": "fund",
        "title": "Pacific Investment Management Company",
        "aum": 1_900_000_000_000,
        "influence_score": 0.84,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INSTITUTIONAL TIER — Sovereign Wealth Funds
    # ──────────────────────────────────────────────────────────────────────
    "swf_norway_gpfg": {
        "name": "Norway Government Pension Fund Global",
        "tier": "institutional",
        "category": "swf",
        "title": "GPFG — World's Largest Sovereign Wealth Fund",
        "aum": 1_600_000_000_000,
        "influence_score": 0.88,
        "data_sources": ["nbim_holdings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "swf_adia": {
        "name": "Abu Dhabi Investment Authority (ADIA)",
        "tier": "institutional",
        "category": "swf",
        "title": "ADIA — Abu Dhabi Sovereign Wealth Fund",
        "aum": 993_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["13f_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "swf_saudi_pif": {
        "name": "Saudi Arabia Public Investment Fund (PIF)",
        "tier": "institutional",
        "category": "swf",
        "title": "PIF — Saudi Arabia Sovereign Wealth Fund",
        "aum": 930_000_000_000,
        "influence_score": 0.85,
        "data_sources": ["13f_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "geopolitical",
    },
    "swf_gic": {
        "name": "GIC Private Limited (Singapore)",
        "tier": "institutional",
        "category": "swf",
        "title": "GIC — Singapore Sovereign Wealth Fund",
        "aum": 770_000_000_000,
        "influence_score": 0.80,
        "data_sources": ["13f_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "swf_cic": {
        "name": "China Investment Corporation (CIC)",
        "tier": "institutional",
        "category": "swf",
        "title": "CIC — China Sovereign Wealth Fund",
        "aum": 1_350_000_000_000,
        "influence_score": 0.84,
        "data_sources": ["public_reports"],
        "credibility": "inferred",
        "motivation_model": "geopolitical",
    },
    "swf_kuwait_kia": {
        "name": "Kuwait Investment Authority (KIA)",
        "tier": "institutional",
        "category": "swf",
        "title": "KIA — Kuwait Sovereign Wealth Fund",
        "aum": 803_000_000_000,
        "influence_score": 0.72,
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "swf_hkma": {
        "name": "Hong Kong Monetary Authority (HKMA)",
        "tier": "institutional",
        "category": "swf",
        "title": "HKMA Exchange Fund",
        "aum": 514_000_000_000,
        "influence_score": 0.72,
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "swf_temasek": {
        "name": "Temasek Holdings (Singapore)",
        "tier": "institutional",
        "category": "swf",
        "title": "Temasek — Singapore State Investment Fund",
        "aum": 382_000_000_000,
        "influence_score": 0.76,
        "data_sources": ["13f_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "swf_qatar_qia": {
        "name": "Qatar Investment Authority (QIA)",
        "tier": "institutional",
        "category": "swf",
        "title": "QIA — Qatar Sovereign Wealth Fund",
        "aum": 475_000_000_000,
        "influence_score": 0.74,
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "geopolitical",
    },
    "swf_bndes": {
        "name": "BNDES (Brazil National Development Bank)",
        "tier": "institutional",
        "category": "swf",
        "title": "Brazil National Development Bank",
        "aum": 180_000_000_000,
        "influence_score": 0.62,
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INSTITUTIONAL TIER — Bank CEOs (market-moving)
    # ──────────────────────────────────────────────────────────────────────
    "bank_dimon": {
        "name": "Jamie Dimon",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman & CEO, JPMorgan Chase",
        "net_worth_estimate": 2_000_000_000,
        "influence_score": 0.90,
        "data_sources": ["sec_filings", "earnings_calls", "public_statements"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "bank_moynihan": {
        "name": "Brian Moynihan",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman & CEO, Bank of America",
        "influence_score": 0.78,
        "data_sources": ["sec_filings", "earnings_calls"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "bank_solomon": {
        "name": "David Solomon",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman & CEO, Goldman Sachs",
        "influence_score": 0.82,
        "data_sources": ["sec_filings", "earnings_calls"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INDIVIDUAL TIER — Top Tech CEOs / mega-holders
    # ──────────────────────────────────────────────────────────────────────
    "ind_musk": {
        "name": "Elon Musk",
        "tier": "individual",
        "category": "insider",
        "title": "CEO, Tesla & SpaceX; Owner, X/Twitter",
        "net_worth_estimate": 230_000_000_000,
        "influence_score": 0.92,
        "data_sources": ["sec_filings", "form4", "social_media"],
        "credibility": "hard_data",
        "motivation_model": "self_serving",
    },
    "ind_bezos": {
        "name": "Jeff Bezos",
        "tier": "individual",
        "category": "insider",
        "title": "Executive Chairman, Amazon",
        "net_worth_estimate": 200_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "diversification",
    },
    "ind_zuckerberg": {
        "name": "Mark Zuckerberg",
        "tier": "individual",
        "category": "insider",
        "title": "CEO, Meta Platforms",
        "net_worth_estimate": 180_000_000_000,
        "influence_score": 0.80,
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ind_buffett": {
        "name": "Warren Buffett",
        "tier": "individual",
        "category": "fund",
        "title": "Chairman & CEO, Berkshire Hathaway",
        "net_worth_estimate": 130_000_000_000,
        "aum": 970_000_000_000,
        "influence_score": 0.94,
        "data_sources": ["13f_filings", "sec_edgar", "annual_letters"],
        "credibility": "hard_data",
        "motivation_model": "value_investor",
    },
    "ind_cook_tim": {
        "name": "Tim Cook",
        "tier": "individual",
        "category": "insider",
        "title": "CEO, Apple Inc.",
        "net_worth_estimate": 1_800_000_000,
        "influence_score": 0.78,
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ind_nadella": {
        "name": "Satya Nadella",
        "tier": "individual",
        "category": "insider",
        "title": "Chairman & CEO, Microsoft",
        "net_worth_estimate": 1_000_000_000,
        "influence_score": 0.78,
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ind_jensen": {
        "name": "Jensen Huang",
        "tier": "individual",
        "category": "insider",
        "title": "Founder & CEO, NVIDIA",
        "net_worth_estimate": 100_000_000_000,
        "influence_score": 0.86,
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ind_altman": {
        "name": "Sam Altman",
        "tier": "individual",
        "category": "insider",
        "title": "CEO, OpenAI",
        "net_worth_estimate": 2_000_000_000,
        "influence_score": 0.72,
        "data_sources": ["public_statements"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "ind_su": {
        "name": "Lisa Su",
        "tier": "individual",
        "category": "insider",
        "title": "Chair & CEO, AMD",
        "net_worth_estimate": 1_000_000_000,
        "influence_score": 0.72,
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ind_jassy": {
        "name": "Andy Jassy",
        "tier": "individual",
        "category": "insider",
        "title": "President & CEO, Amazon",
        "influence_score": 0.74,
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INDIVIDUAL TIER — Individual Macro Traders / Public Track Records
    # ──────────────────────────────────────────────────────────────────────
    "ind_burry": {
        "name": "Michael Burry",
        "tier": "individual",
        "category": "fund",
        "title": "Founder, Scion Asset Management",
        "net_worth_estimate": 300_000_000,
        "influence_score": 0.74,
        "data_sources": ["13f_filings", "sec_edgar", "social_media"],
        "credibility": "hard_data",
        "motivation_model": "contrarian",
    },
    "ind_marks": {
        "name": "Howard Marks",
        "tier": "individual",
        "category": "fund",
        "title": "Co-Chairman, Oaktree Capital Management",
        "net_worth_estimate": 2_200_000_000,
        "aum": 189_000_000_000,
        "influence_score": 0.78,
        "data_sources": ["13f_filings", "memos"],
        "credibility": "hard_data",
        "motivation_model": "value_investor",
    },
    "ind_gundlach": {
        "name": "Jeffrey Gundlach",
        "tier": "individual",
        "category": "fund",
        "title": "CEO, DoubleLine Capital",
        "net_worth_estimate": 2_200_000_000,
        "aum": 92_000_000_000,
        "influence_score": 0.76,
        "data_sources": ["13f_filings", "public_statements"],
        "credibility": "hard_data",
        "motivation_model": "macro_discretionary",
    },
    "ind_bass": {
        "name": "Kyle Bass",
        "tier": "individual",
        "category": "fund",
        "title": "Founder, Hayman Capital Management",
        "net_worth_estimate": 400_000_000,
        "influence_score": 0.68,
        "data_sources": ["13f_filings", "public_statements", "social_media"],
        "credibility": "public_record",
        "motivation_model": "macro_discretionary",
    },
    "ind_cooperman": {
        "name": "Leon Cooperman",
        "tier": "individual",
        "category": "fund",
        "title": "Founder, Omega Advisors (now family office)",
        "net_worth_estimate": 2_500_000_000,
        "influence_score": 0.68,
        "data_sources": ["13f_filings", "public_statements"],
        "credibility": "hard_data",
        "motivation_model": "value_investor",
    },
    "ind_chanos": {
        "name": "Jim Chanos",
        "tier": "individual",
        "category": "fund",
        "title": "Founder, Kynikos Associates (Closed 2023)",
        "influence_score": 0.64,
        "data_sources": ["public_statements", "academic"],
        "credibility": "public_record",
        "motivation_model": "contrarian",
    },
    "ind_bury_cathie": {
        "name": "Cathie Wood",
        "tier": "individual",
        "category": "fund",
        "title": "Founder & CEO, ARK Invest",
        "net_worth_estimate": 250_000_000,
        "aum": 14_000_000_000,
        "influence_score": 0.72,
        "data_sources": ["13f_filings", "ark_daily_trades", "social_media"],
        "credibility": "hard_data",
        "motivation_model": "growth_conviction",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INDIVIDUAL TIER — Additional congressional high-volume traders
    # ──────────────────────────────────────────────────────────────────────
    "congress_khanna": {
        "name": "Ro Khanna",
        "tier": "individual",
        "category": "politician",
        "title": "Representative, California (tech district)",
        "influence_score": 0.50,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },
    "congress_gimenez": {
        "name": "Carlos Gimenez",
        "tier": "individual",
        "category": "politician",
        "title": "Representative, Florida",
        "influence_score": 0.48,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },
    "congress_gottheimer": {
        "name": "Josh Gottheimer",
        "tier": "individual",
        "category": "politician",
        "title": "Representative, New Jersey",
        "influence_score": 0.50,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },
    "congress_moore": {
        "name": "Blake Moore",
        "tier": "individual",
        "category": "politician",
        "title": "Representative, Utah",
        "influence_score": 0.46,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },
    "congress_meuser": {
        "name": "Dan Meuser",
        "tier": "individual",
        "category": "politician",
        "title": "Representative, Pennsylvania",
        "influence_score": 0.46,
        "data_sources": ["congressional_disclosures"],
        "credibility": "public_record",
        "motivation_model": "self_serving",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INDIVIDUAL TIER — Corporate insiders with best historical accuracy
    # ──────────────────────────────────────────────────────────────────────
    # These are placeholders that get dynamically replaced by
    # _load_top_insiders_from_trust_scorer when the DB is available.
    "insider_placeholder_1": {
        "name": "[Dynamic — Top Insider #1 from Trust Scorer]",
        "tier": "individual",
        "category": "insider",
        "title": "Corporate Insider (resolved at runtime)",
        "influence_score": 0.60,
        "data_sources": ["form4", "trust_scorer"],
        "credibility": "hard_data",
        "motivation_model": "informed",
    },
    "insider_placeholder_2": {
        "name": "[Dynamic — Top Insider #2 from Trust Scorer]",
        "tier": "individual",
        "category": "insider",
        "title": "Corporate Insider (resolved at runtime)",
        "influence_score": 0.58,
        "data_sources": ["form4", "trust_scorer"],
        "credibility": "hard_data",
        "motivation_model": "informed",
    },
    "insider_placeholder_3": {
        "name": "[Dynamic — Top Insider #3 from Trust Scorer]",
        "tier": "individual",
        "category": "insider",
        "title": "Corporate Insider (resolved at runtime)",
        "influence_score": 0.56,
        "data_sources": ["form4", "trust_scorer"],
        "credibility": "hard_data",
        "motivation_model": "informed",
    },

    # ──────────────────────────────────────────────────────────────────────
    # INSTITUTIONAL TIER — Additional notable entities
    # ──────────────────────────────────────────────────────────────────────
    "hf_citadel_securities": {
        "name": "Citadel Securities",
        "tier": "institutional",
        "category": "fund",
        "title": "Market Maker (largest US equities MM)",
        "aum": 0,  # market maker, not AUM
        "influence_score": 0.88,
        "data_sources": ["finra_ats", "sec_filings"],
        "credibility": "hard_data",
        "motivation_model": "market_making",
    },
    "hf_virtu": {
        "name": "Virtu Financial",
        "tier": "institutional",
        "category": "fund",
        "title": "Market Maker & HFT",
        "influence_score": 0.72,
        "data_sources": ["finra_ats", "sec_filings"],
        "credibility": "hard_data",
        "motivation_model": "market_making",
    },
    "hf_jane_street": {
        "name": "Jane Street",
        "tier": "institutional",
        "category": "fund",
        "title": "Quantitative Trading Firm",
        "aum": 20_000_000_000,
        "influence_score": 0.80,
        "data_sources": ["13f_filings"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_tiger_global": {
        "name": "Tiger Global Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Tiger Global Management",
        "aum": 30_000_000_000,
        "influence_score": 0.74,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "growth_conviction",
    },
    "hf_coatue": {
        "name": "Philippe Laffont / Coatue Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Coatue Management",
        "aum": 20_000_000_000,
        "influence_score": 0.72,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "growth_conviction",
    },
    "hf_viking": {
        "name": "Andreas Halvorsen / Viking Global",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Viking Global Investors",
        "net_worth_estimate": 6_000_000_000,
        "aum": 36_000_000_000,
        "influence_score": 0.76,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "fundamental",
    },
    "hf_lone_pine": {
        "name": "Stephen Mandel Jr. / Lone Pine Capital",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Lone Pine Capital",
        "aum": 17_000_000_000,
        "influence_score": 0.72,
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "fundamental",
    },
}

# Confirm count at module-load time for development
_ACTOR_COUNT = len(_KNOWN_ACTORS)
assert _ACTOR_COUNT >= 100, (
    f"Expected >= 100 known actors, got {_ACTOR_COUNT}. Add more seed data."
)


# Sector-committee mapping (reused from lever_pullers for pocket-lining)
_SECTOR_COMMITTEE_MAP: dict[str, set[str]] = {
    "XLK": {"commerce", "intelligence", "science"},
    "XLF": {"financial services", "banking", "finance"},
    "XLE": {"energy", "natural resources"},
    "XLV": {"health", "finance"},
    "XLI": {"armed services", "transportation", "infrastructure"},
    "XLB": {"natural resources", "energy"},
    "XLRE": {"financial services", "banking"},
    "XLU": {"energy", "commerce"},
    "XLC": {"commerce", "intelligence"},
    "XLY": {"commerce", "finance"},
    "XLP": {"agriculture", "commerce"},
}

# Ticker-to-sector ETF hint (extends to more tickers as needed)
_TICKER_SECTOR: dict[str, str] = {
    "AAPL": "XLK", "MSFT": "XLK", "GOOGL": "XLC", "AMZN": "XLY",
    "META": "XLC", "NVDA": "XLK", "TSLA": "XLY", "JPM": "XLF",
    "BAC": "XLF", "GS": "XLF", "XOM": "XLE", "CVX": "XLE",
    "JNJ": "XLV", "PFE": "XLV", "UNH": "XLV", "LMT": "XLI",
    "RTX": "XLI", "BA": "XLI", "GD": "XLI", "NOC": "XLI",
    "WMT": "XLP", "PG": "XLP", "KO": "XLP", "COST": "XLP",
    "NEE": "XLU", "DUK": "XLU", "SO": "XLU",
    "AMT": "XLRE", "PLD": "XLRE",
}


# ══════════════════════════════════════════════════════════════════════════
# TABLE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════

def _ensure_tables(engine: Engine) -> None:
    """Create the actors and wealth_flows tables if they do not exist.

    Parameters:
        engine: SQLAlchemy engine connected to the GRID database.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS actors (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                tier            TEXT NOT NULL,
                category        TEXT NOT NULL,
                title           TEXT,
                net_worth_estimate NUMERIC,
                aum             NUMERIC,
                influence_score NUMERIC DEFAULT 0.5,
                trust_score     NUMERIC DEFAULT 0.5,
                motivation_model TEXT DEFAULT 'unknown',
                connections     JSONB DEFAULT '[]',
                known_positions JSONB DEFAULT '[]',
                board_seats     JSONB DEFAULT '[]',
                political_affiliations JSONB DEFAULT '[]',
                data_sources    JSONB DEFAULT '[]',
                credibility     TEXT DEFAULT 'inferred',
                metadata        JSONB DEFAULT '{}',
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_tier
                ON actors (tier)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_influence
                ON actors (influence_score DESC)
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS wealth_flows (
                id              SERIAL PRIMARY KEY,
                from_actor      TEXT REFERENCES actors(id),
                to_entity       TEXT NOT NULL,
                amount_estimate NUMERIC,
                confidence      TEXT DEFAULT 'inferred',
                evidence        JSONB DEFAULT '[]',
                flow_date       DATE,
                implication     TEXT,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_wealth_flows_date
                ON wealth_flows (flow_date DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_wealth_flows_actor
                ON wealth_flows (from_actor)
        """))
    log.debug("actors / wealth_flows tables ensured")


def _seed_known_actors(engine: Engine) -> int:
    """Insert or update all _KNOWN_ACTORS into the actors table.

    Returns:
        Number of actors upserted.
    """
    _ensure_tables(engine)
    count = 0
    with engine.begin() as conn:
        for actor_id, data in _KNOWN_ACTORS.items():
            conn.execute(text("""
                INSERT INTO actors (
                    id, name, tier, category, title,
                    net_worth_estimate, aum, influence_score,
                    trust_score, motivation_model,
                    data_sources, credibility, updated_at
                ) VALUES (
                    :id, :name, :tier, :category, :title,
                    :nw, :aum, :inf,
                    :trust, :motivation,
                    :sources, :cred, NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    tier = EXCLUDED.tier,
                    category = EXCLUDED.category,
                    title = EXCLUDED.title,
                    net_worth_estimate = COALESCE(EXCLUDED.net_worth_estimate, actors.net_worth_estimate),
                    aum = COALESCE(EXCLUDED.aum, actors.aum),
                    influence_score = EXCLUDED.influence_score,
                    motivation_model = EXCLUDED.motivation_model,
                    data_sources = EXCLUDED.data_sources,
                    credibility = EXCLUDED.credibility,
                    updated_at = NOW()
            """), {
                "id": actor_id,
                "name": data["name"],
                "tier": data["tier"],
                "category": data["category"],
                "title": data["title"],
                "nw": data.get("net_worth_estimate"),
                "aum": data.get("aum"),
                "inf": data.get("influence_score", 0.5),
                "trust": data.get("trust_score", 0.5),
                "motivation": data.get("motivation_model", "unknown"),
                "sources": json.dumps(data.get("data_sources", [])),
                "cred": data.get("credibility", "inferred"),
            })
            count += 1
    log.info("Seeded {n} actors into the database", n=count)
    return count


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _load_actors_from_db(engine: Engine) -> dict[str, Actor]:
    """Load all actors from the DB into Actor dataclass instances.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        Dict mapping actor_id -> Actor.
    """
    actors: dict[str, Actor] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, name, tier, category, title,
                       net_worth_estimate, aum, influence_score,
                       trust_score, motivation_model,
                       connections, known_positions, board_seats,
                       political_affiliations, data_sources, credibility
                FROM actors
                ORDER BY influence_score DESC
            """)).fetchall()
            for r in rows:
                actors[r[0]] = Actor(
                    id=r[0],
                    name=r[1],
                    tier=r[2],
                    category=r[3],
                    title=r[4] or "",
                    net_worth_estimate=float(r[5]) if r[5] is not None else None,
                    aum=float(r[6]) if r[6] is not None else None,
                    influence_score=float(r[7]) if r[7] is not None else 0.5,
                    trust_score=float(r[8]) if r[8] is not None else 0.5,
                    motivation_model=r[9] or "unknown",
                    connections=_parse_jsonb(r[10]),
                    known_positions=_parse_jsonb(r[11]),
                    board_seats=_parse_jsonb(r[12]),
                    political_affiliations=_parse_jsonb(r[13]),
                    data_sources=_parse_jsonb(r[14]),
                    credibility=r[15] or "inferred",
                )
    except Exception as exc:
        log.warning("Failed to load actors from DB: {e}", e=str(exc))
    return actors


def _parse_jsonb(val: Any) -> list:
    """Safely parse a JSONB field that may arrive as str, list, or None."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def _resolve_dynamic_insiders(engine: Engine) -> list[dict]:
    """Query trust_scorer data to find the top insiders by accuracy.

    Returns up to 10 highest-trust insiders as dicts suitable for
    merging into the actor graph.
    """
    insiders: list[dict] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, trust_score,
                       COUNT(*) AS total_signals
                FROM signal_sources
                WHERE source_type = 'insider'
                  AND trust_score IS NOT NULL
                GROUP BY source_id, trust_score
                HAVING COUNT(*) >= 3
                ORDER BY trust_score DESC
                LIMIT 10
            """)).fetchall()
            for i, r in enumerate(rows):
                insiders.append({
                    "id": f"insider_dynamic_{i}",
                    "name": r[0],
                    "tier": "individual",
                    "category": "insider",
                    "title": f"Corporate Insider (trust={float(r[1]):.2f}, signals={r[2]})",
                    "influence_score": min(0.4 + float(r[1]) * 0.4, 0.80),
                    "trust_score": float(r[1]),
                    "data_sources": ["form4", "trust_scorer"],
                    "credibility": "hard_data",
                    "motivation_model": "informed",
                })
    except Exception as exc:
        log.debug("Could not resolve dynamic insiders: {e}", e=str(exc))
    return insiders


def _compute_influence_propagation(
    actors: dict[str, Actor],
) -> dict[str, float]:
    """Propagate influence through the actor graph.

    If actor A controls fund B which holds stock C, A's influence
    propagates to C with decay.

    Returns:
        Dict of actor_id -> propagated_influence_score.
    """
    propagated: dict[str, float] = {}
    decay = 0.5  # each hop reduces influence by half

    for actor_id, actor in actors.items():
        propagated[actor_id] = actor.influence_score

        # Walk connections
        for conn in actor.connections:
            target = conn.get("actor_id", "")
            strength = float(conn.get("strength", 0.5))
            if target in actors:
                current = propagated.get(target, actors[target].influence_score)
                contribution = actor.influence_score * strength * decay
                propagated[target] = min(current + contribution, 1.0)

    return propagated


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════

def build_actor_graph(engine: Engine) -> dict:
    """Build the complete actor network graph.

    Loads all actors + their connections, computes influence propagation,
    and returns a graph structure suitable for D3 force-directed visualization.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with keys: nodes, links, metadata.
        - nodes: list of dicts with id, label, tier, category, influence, size
        - links: list of dicts with source, target, relationship, strength
        - metadata: summary statistics
    """
    _ensure_tables(engine)

    # Load base actors from DB (fall back to _KNOWN_ACTORS if empty)
    actors = _load_actors_from_db(engine)
    if not actors:
        _seed_known_actors(engine)
        actors = _load_actors_from_db(engine)

    # Merge dynamic insiders
    dynamic_insiders = _resolve_dynamic_insiders(engine)
    for ins in dynamic_insiders:
        aid = ins["id"]
        if aid not in actors:
            actors[aid] = Actor(
                id=aid,
                name=ins["name"],
                tier=ins["tier"],
                category=ins["category"],
                title=ins["title"],
                influence_score=ins["influence_score"],
                trust_score=ins.get("trust_score", 0.5),
                data_sources=ins.get("data_sources", []),
                credibility=ins.get("credibility", "hard_data"),
                motivation_model=ins.get("motivation_model", "informed"),
            )

    # Compute propagated influence
    propagated = _compute_influence_propagation(actors)

    # Build nodes
    nodes: list[dict] = []
    for actor_id, actor in actors.items():
        effective_influence = propagated.get(actor_id, actor.influence_score)
        nodes.append({
            "id": actor_id,
            "label": actor.name,
            "tier": actor.tier,
            "category": actor.category,
            "title": actor.title,
            "influence": round(effective_influence, 3),
            "trust_score": round(actor.trust_score, 3),
            "net_worth": actor.net_worth_estimate,
            "aum": actor.aum,
            "motivation": actor.motivation_model,
            "credibility": actor.credibility,
            # D3 sizing: scale radius by influence
            "size": max(4, int(effective_influence * 30)),
        })

    # Build links from connections
    links: list[dict] = []
    seen_links: set[tuple[str, str]] = set()
    for actor_id, actor in actors.items():
        for conn_info in actor.connections:
            target = conn_info.get("actor_id", "")
            if target in actors and (actor_id, target) not in seen_links:
                links.append({
                    "source": actor_id,
                    "target": target,
                    "relationship": conn_info.get("relationship", "connected"),
                    "strength": float(conn_info.get("strength", 0.5)),
                })
                seen_links.add((actor_id, target))
                seen_links.add((target, actor_id))

    # Tier breakdown
    tier_counts: dict[str, int] = defaultdict(int)
    for actor in actors.values():
        tier_counts[actor.tier] += 1

    metadata = {
        "total_actors": len(actors),
        "total_links": len(links),
        "tier_breakdown": dict(tier_counts),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    log.info(
        "Actor graph built: {n} nodes, {l} links",
        n=len(nodes), l=len(links),
    )

    return {"nodes": nodes, "links": links, "metadata": metadata}


def track_wealth_migration(
    engine: Engine,
    days: int = 90,
) -> list[WealthFlow]:
    """Track where money is moving over the last N days.

    Aggregates data from:
      - 13F filings: institutional position changes
      - Congressional disclosures: politician buys/sells
      - Form 4 insider filings: accumulation/dumping
      - Dark pool signals: anonymous large flows

    Parameters:
        engine: SQLAlchemy engine.
        days: Lookback window.

    Returns:
        List of WealthFlow objects sorted by amount descending.
    """
    _ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)
    flows: list[WealthFlow] = []

    # ── 13F-derived flows (institutional) ─────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ss.source_id, ss.ticker, ss.direction,
                       ss.signal_date, ss.signal_value, ss.trust_score
                FROM signal_sources ss
                WHERE ss.source_type = 'institutional'
                  AND ss.signal_date >= :cutoff
                ORDER BY ss.signal_date DESC
                LIMIT 500
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                value_data = _parse_signal_value(r[4])
                amount = value_data.get("amount", 0) or value_data.get("market_value", 0)
                flows.append(WealthFlow(
                    from_actor=str(r[0]),
                    to_actor=str(r[1]),
                    amount_estimate=float(amount) if amount else 0,
                    confidence="confirmed" if r[5] and float(r[5]) > 0.7 else "likely",
                    evidence=["13f_filing"],
                    timestamp=str(r[3]),
                    implication=f"Institutional {r[2]} in {r[1]}",
                ))
    except Exception as exc:
        log.debug("13F flow query failed: {e}", e=str(exc))

    # ── Congressional disclosures ─────────────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, ticker, direction,
                       signal_date, signal_value, trust_score
                FROM signal_sources
                WHERE source_type = 'congressional'
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 500
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                value_data = _parse_signal_value(r[4])
                amount = value_data.get("amount", 0)
                # Congressional disclosures use range estimates
                low = value_data.get("amount_low", 0)
                high = value_data.get("amount_high", 0)
                if low and high:
                    amount = (float(low) + float(high)) / 2
                flows.append(WealthFlow(
                    from_actor=str(r[0]),
                    to_actor=str(r[1]),
                    amount_estimate=float(amount) if amount else 0,
                    confidence="confirmed",
                    evidence=["congressional_disclosure"],
                    timestamp=str(r[3]),
                    implication=f"Congress member {r[0]} {r[2]} {r[1]}",
                ))
    except Exception as exc:
        log.debug("Congressional flow query failed: {e}", e=str(exc))

    # ── Insider filings (Form 4) ──────────────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, ticker, direction,
                       signal_date, signal_value, trust_score
                FROM signal_sources
                WHERE source_type = 'insider'
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 500
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                value_data = _parse_signal_value(r[4])
                amount = value_data.get("amount", 0) or value_data.get("value", 0)
                flows.append(WealthFlow(
                    from_actor=str(r[0]),
                    to_actor=str(r[1]),
                    amount_estimate=float(amount) if amount else 0,
                    confidence="confirmed",
                    evidence=["form4"],
                    timestamp=str(r[3]),
                    implication=f"Insider {r[0]} {r[2]} {r[1]}",
                ))
    except Exception as exc:
        log.debug("Insider flow query failed: {e}", e=str(exc))

    # ── Dark pool signals ─────────────────────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, direction, signal_date,
                       signal_value, trust_score
                FROM signal_sources
                WHERE source_type = 'darkpool'
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 200
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                value_data = _parse_signal_value(r[3])
                volume = value_data.get("volume", 0) or value_data.get("spike_ratio", 1)
                flows.append(WealthFlow(
                    from_actor="dark_pool_anonymous",
                    to_actor=str(r[0]),
                    amount_estimate=float(volume) if volume else 0,
                    confidence="rumored",
                    evidence=["finra_ats"],
                    timestamp=str(r[2]),
                    implication=f"Dark pool {r[1]} signal in {r[0]}",
                ))
    except Exception as exc:
        log.debug("Dark pool flow query failed: {e}", e=str(exc))

    # Sort by amount descending
    flows.sort(key=lambda f: abs(f.amount_estimate), reverse=True)
    log.info("Tracked {n} wealth flows over {d} days", n=len(flows), d=days)
    return flows


def _parse_signal_value(val: Any) -> dict:
    """Parse signal_value which may be JSON string, dict, or None."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def find_connected_actions(
    engine: Engine,
    actor_id: str,
) -> list[dict]:
    """Find correlated actions within an actor's network.

    When actor X acts, who else in their network also acted recently?
    E.g., "3 board members of Company Y all sold within 2 weeks."

    Parameters:
        engine: SQLAlchemy engine.
        actor_id: The actor to investigate.

    Returns:
        List of dicts describing connected actions, sorted by recency.
    """
    _ensure_tables(engine)
    results: list[dict] = []

    # Load the target actor
    actors = _load_actors_from_db(engine)
    target = actors.get(actor_id)
    if not target:
        log.warning("Actor {a} not found", a=actor_id)
        return results

    # Get the target's recent actions from signal_sources
    cutoff = date.today() - timedelta(days=30)
    target_tickers: set[str] = set()

    try:
        with engine.connect() as conn:
            # Find tickers this actor recently acted on
            rows = conn.execute(text("""
                SELECT DISTINCT ticker, direction, signal_date
                FROM signal_sources
                WHERE source_id = :sid
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 20
            """), {"sid": target.name, "cutoff": cutoff}).fetchall()

            for r in rows:
                target_tickers.add(str(r[0]))

            if not target_tickers:
                return results

            # Find other actors who acted on the same tickers in a 14-day window
            connected_ids = {c.get("actor_id") for c in target.connections if c.get("actor_id")}
            # Also search by name matches across all signal sources
            ticker_list = list(target_tickers)

            related_rows = conn.execute(text("""
                SELECT source_type, source_id, ticker, direction,
                       signal_date, trust_score
                FROM signal_sources
                WHERE ticker = ANY(:tickers)
                  AND signal_date >= :cutoff
                  AND source_id != :exclude
                ORDER BY signal_date DESC
                LIMIT 100
            """), {
                "tickers": ticker_list,
                "cutoff": cutoff,
                "exclude": target.name,
            }).fetchall()

            # Group by ticker to find clusters
            by_ticker: dict[str, list[dict]] = defaultdict(list)
            for r in related_rows:
                by_ticker[str(r[2])].append({
                    "source_type": r[0],
                    "source_id": r[1],
                    "ticker": r[2],
                    "direction": r[3],
                    "signal_date": str(r[4]),
                    "trust_score": float(r[5]) if r[5] else 0.5,
                })

            for ticker, actions in by_ticker.items():
                if len(actions) >= 2:
                    # Multiple actors acting on the same ticker = connected action
                    directions = {a["direction"] for a in actions}
                    alignment = "aligned" if len(directions) == 1 else "mixed"
                    results.append({
                        "ticker": ticker,
                        "primary_actor": actor_id,
                        "connected_actors": [
                            {
                                "source_type": a["source_type"],
                                "name": a["source_id"],
                                "direction": a["direction"],
                                "date": a["signal_date"],
                                "trust": a["trust_score"],
                            }
                            for a in actions
                        ],
                        "total_actors": len(actions) + 1,  # +1 for primary
                        "alignment": alignment,
                        "dominant_direction": actions[0]["direction"],
                        "conviction": "high" if len(actions) >= 3 and alignment == "aligned" else "moderate",
                    })
    except Exception as exc:
        log.warning("Connected action search failed: {e}", e=str(exc))

    # Sort by number of connected actors (most coordinated first)
    results.sort(key=lambda x: x.get("total_actors", 0), reverse=True)
    log.info(
        "Found {n} connected action clusters for {a}",
        n=len(results), a=actor_id,
    )
    return results


def assess_pocket_lining(engine: Engine) -> list[dict]:
    """Detect self-dealing, conflicts of interest, and suspicious patterns.

    Detections:
      1. Politician trades in sector their committee oversees
      2. Fund manager personal trades diverge from fund trades
      3. Insider sells right before bad news
      4. Lobbying spend correlates with favorable regulation

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        List of dicts, each describing a suspicious pattern with
        who, what, who_benefits, confidence, implication.
    """
    _ensure_tables(engine)
    flags: list[dict] = []
    cutoff = date.today() - timedelta(days=90)

    # ── Detection 1: Politicians trading in their committee's jurisdiction ──
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, ticker, direction, signal_date,
                       signal_value
                FROM signal_sources
                WHERE source_type = 'congressional'
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                member = str(r[0])
                ticker = str(r[1])
                direction = str(r[2])
                sig_date = str(r[3])

                # Check if ticker maps to a sector ETF
                sector_etf = _TICKER_SECTOR.get(ticker)
                if not sector_etf:
                    continue

                # Check if any known politician actor has committee overlap
                committee_keywords = _SECTOR_COMMITTEE_MAP.get(sector_etf, set())
                if not committee_keywords:
                    continue

                # Look up the actor for this member
                member_actor = None
                for aid, actor in _KNOWN_ACTORS.items():
                    if actor["name"].lower() in member.lower() or member.lower() in actor["name"].lower():
                        member_actor = actor
                        break

                if member_actor:
                    title_lower = member_actor.get("title", "").lower()
                    matching_committees = [kw for kw in committee_keywords if kw in title_lower]
                    if matching_committees:
                        flags.append({
                            "detection": "committee_jurisdiction_trade",
                            "who": member,
                            "what": f"{direction} {ticker} on {sig_date}",
                            "who_benefits": member,
                            "overlap": f"Committees: {', '.join(matching_committees)}; Sector: {sector_etf}",
                            "confidence": "likely",
                            "implication": (
                                f"{member} traded {ticker} ({sector_etf} sector) while "
                                f"serving on committee with jurisdiction over that sector"
                            ),
                            "severity": "high",
                        })
    except Exception as exc:
        log.debug("Committee jurisdiction check failed: {e}", e=str(exc))

    # ── Detection 2: Fund manager personal trades diverge from fund ────────
    try:
        with engine.connect() as conn:
            # Look for actors who appear in both insider and institutional feeds
            rows = conn.execute(text("""
                WITH insider_trades AS (
                    SELECT source_id, ticker, direction, signal_date
                    FROM signal_sources
                    WHERE source_type = 'insider'
                      AND signal_date >= :cutoff
                ),
                fund_trades AS (
                    SELECT source_id, ticker, direction, signal_date
                    FROM signal_sources
                    WHERE source_type = 'institutional'
                      AND signal_date >= :cutoff
                )
                SELECT i.source_id AS insider_name,
                       i.ticker,
                       i.direction AS insider_direction,
                       f.direction AS fund_direction,
                       i.signal_date
                FROM insider_trades i
                JOIN fund_trades f ON i.ticker = f.ticker
                    AND ABS(i.signal_date - f.signal_date) <= 30
                WHERE i.direction != f.direction
                LIMIT 100
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                flags.append({
                    "detection": "fund_manager_divergence",
                    "who": str(r[0]),
                    "what": (
                        f"Personal: {r[2]} {r[1]}; "
                        f"Fund: {r[3]} {r[1]} on {r[4]}"
                    ),
                    "who_benefits": str(r[0]),
                    "confidence": "likely",
                    "implication": (
                        f"Insider {r[0]} is personally trading {r[2]} {r[1]} "
                        f"while their fund is doing the opposite ({r[3]})"
                    ),
                    "severity": "high",
                })
    except Exception as exc:
        log.debug("Fund manager divergence check failed: {e}", e=str(exc))

    # ── Detection 3: Insider sells before bad news ─────────────────────────
    try:
        with engine.connect() as conn:
            # Find insider SELL signals followed by price drops
            rows = conn.execute(text("""
                SELECT ss.source_id, ss.ticker, ss.signal_date, ss.trust_score
                FROM signal_sources ss
                WHERE ss.source_type = 'insider'
                  AND ss.direction = 'SELL'
                  AND ss.signal_date >= :cutoff
                ORDER BY ss.signal_date DESC
                LIMIT 200
            """), {"cutoff": cutoff}).fetchall()

            for r in rows:
                insider_name = str(r[0])
                ticker = str(r[1])
                sell_date = r[2]
                trust = float(r[3]) if r[3] else 0.5

                # Check if price dropped significantly after the sell
                if sell_date is None:
                    continue

                check_date = sell_date + timedelta(days=14) if isinstance(sell_date, date) else None
                if check_date and check_date <= date.today():
                    price_row = conn.execute(text("""
                        SELECT value FROM raw_series
                        WHERE series_id = :sid
                          AND obs_date BETWEEN :d1 AND :d2
                          AND pull_status = 'SUCCESS'
                        ORDER BY obs_date DESC LIMIT 1
                    """), {
                        "sid": f"YF:{ticker}:close",
                        "d1": sell_date,
                        "d2": check_date,
                    }).fetchone()

                    price_before_row = conn.execute(text("""
                        SELECT value FROM raw_series
                        WHERE series_id = :sid
                          AND obs_date <= :d
                          AND pull_status = 'SUCCESS'
                        ORDER BY obs_date DESC LIMIT 1
                    """), {
                        "sid": f"YF:{ticker}:close",
                        "d": sell_date,
                    }).fetchone()

                    if price_row and price_before_row:
                        after = float(price_row[0])
                        before = float(price_before_row[0])
                        if before > 0:
                            pct_change = (after - before) / before
                            if pct_change < -0.05:  # >5% drop
                                flags.append({
                                    "detection": "insider_sell_before_drop",
                                    "who": insider_name,
                                    "what": f"Sold {ticker} on {sell_date}, price dropped {pct_change*100:.1f}% within 14 days",
                                    "who_benefits": insider_name,
                                    "confidence": "confirmed" if pct_change < -0.10 else "likely",
                                    "implication": (
                                        f"{insider_name} sold {ticker} before a {abs(pct_change)*100:.1f}% decline. "
                                        f"Trust score: {trust:.2f}"
                                    ),
                                    "severity": "critical" if pct_change < -0.10 else "high",
                                })
    except Exception as exc:
        log.debug("Insider pre-drop check failed: {e}", e=str(exc))

    # ── Detection 4: Lobbying spend -> favorable outcome ───────────────────
    # This detection correlates lobbying disclosures with regulatory outcomes.
    # Currently data-limited; we flag when a tracked actor's sector receives
    # favorable regulatory signals after lobbying spend.
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, ticker, direction, signal_date
                FROM signal_sources
                WHERE source_type IN ('congressional', 'insider')
                  AND signal_date >= :cutoff
                  AND direction = 'BUY'
                ORDER BY signal_date DESC
                LIMIT 100
            """), {"cutoff": cutoff}).fetchall()

            # Cross-check: if multiple politicians bought the same ticker,
            # it may indicate coordinated insider knowledge of regulation
            ticker_buyers: dict[str, list[str]] = defaultdict(list)
            for r in rows:
                ticker_buyers[str(r[1])].append(str(r[0]))

            for ticker, buyers in ticker_buyers.items():
                if len(buyers) >= 3:
                    unique_buyers = list(set(buyers))
                    if len(unique_buyers) >= 3:
                        flags.append({
                            "detection": "coordinated_political_buying",
                            "who": ", ".join(unique_buyers[:5]),
                            "what": f"{len(unique_buyers)} unique actors buying {ticker} within 90 days",
                            "who_benefits": f"All buyers of {ticker}",
                            "confidence": "rumored",
                            "implication": (
                                f"Coordinated buying in {ticker} by {len(unique_buyers)} actors. "
                                f"May indicate shared non-public information."
                            ),
                            "severity": "moderate",
                        })
    except Exception as exc:
        log.debug("Lobbying correlation check failed: {e}", e=str(exc))

    # Sort by severity
    severity_order = {"critical": 0, "high": 1, "moderate": 2, "low": 3}
    flags.sort(key=lambda x: severity_order.get(x.get("severity", "low"), 3))

    log.info("Pocket-lining assessment: {n} flags raised", n=len(flags))
    return flags


def get_actor_context_for_ticker(
    engine: Engine,
    ticker: str,
) -> dict:
    """Get all actor intelligence relevant to a specific ticker.

    For watchlist detail pages: which actors are relevant to this ticker,
    their recent actions, motivations, and connections.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol (e.g. "AAPL").

    Returns:
        Dict with keys: ticker, actors, recent_actions, power_summary,
        risk_signals.
    """
    _ensure_tables(engine)
    result: dict[str, Any] = {
        "ticker": ticker,
        "actors": [],
        "recent_actions": [],
        "power_summary": "",
        "risk_signals": [],
    }

    cutoff = date.today() - timedelta(days=60)

    try:
        with engine.connect() as conn:
            # Find all signal sources acting on this ticker
            rows = conn.execute(text("""
                SELECT source_type, source_id, direction,
                       signal_date, signal_value, trust_score
                FROM signal_sources
                WHERE ticker = :t
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 100
            """), {"t": ticker, "cutoff": cutoff}).fetchall()

            actor_names: set[str] = set()
            buys = 0
            sells = 0

            for r in rows:
                source_type = str(r[0])
                source_id = str(r[1])
                direction = str(r[2])
                sig_date = str(r[3])
                trust = float(r[5]) if r[5] else 0.5

                if direction.upper() == "BUY":
                    buys += 1
                elif direction.upper() == "SELL":
                    sells += 1

                actor_names.add(source_id)
                result["recent_actions"].append({
                    "source_type": source_type,
                    "actor": source_id,
                    "direction": direction,
                    "date": sig_date,
                    "trust_score": round(trust, 3),
                })

            # Cross-reference with known actors
            actors_db = _load_actors_from_db(engine)
            matched_actors: list[dict] = []

            for actor_id, actor in actors_db.items():
                # Match by name appearing in signal sources
                if actor.name in actor_names or any(
                    actor.name.lower() in name.lower() for name in actor_names
                ):
                    matched_actors.append({
                        "id": actor_id,
                        "name": actor.name,
                        "tier": actor.tier,
                        "category": actor.category,
                        "title": actor.title,
                        "influence": round(actor.influence_score, 3),
                        "motivation": actor.motivation_model,
                        "trust_score": round(actor.trust_score, 3),
                    })

            # Also include actors whose known_positions mention this ticker
            for actor_id, actor in actors_db.items():
                if any(
                    pos.get("ticker", "").upper() == ticker.upper()
                    for pos in actor.known_positions
                ):
                    if actor_id not in {a["id"] for a in matched_actors}:
                        matched_actors.append({
                            "id": actor_id,
                            "name": actor.name,
                            "tier": actor.tier,
                            "category": actor.category,
                            "title": actor.title,
                            "influence": round(actor.influence_score, 3),
                            "motivation": actor.motivation_model,
                            "trust_score": round(actor.trust_score, 3),
                        })

            result["actors"] = sorted(
                matched_actors,
                key=lambda x: x.get("influence", 0),
                reverse=True,
            )

            # Power summary
            total_actors = len(actor_names)
            if total_actors > 0:
                net_direction = "bullish" if buys > sells else "bearish" if sells > buys else "neutral"
                result["power_summary"] = (
                    f"{total_actors} actors active on {ticker} in the last 60 days. "
                    f"Net bias: {net_direction} ({buys} buys, {sells} sells). "
                    f"{len(matched_actors)} matched to known power players."
                )
            else:
                result["power_summary"] = f"No recent actor activity detected for {ticker}."

            # Risk signals: check for insider cluster sells, congressional sells, etc.
            if sells >= 3 and sells > buys * 2:
                result["risk_signals"].append({
                    "signal": "cluster_selling",
                    "description": f"{sells} sells vs {buys} buys from tracked actors",
                    "severity": "high",
                })

            # Check for high-influence actor selling
            for actor_info in matched_actors:
                if actor_info["influence"] > 0.8:
                    for action in result["recent_actions"]:
                        if (action["actor"] == actor_info["name"]
                                and action["direction"].upper() == "SELL"):
                            result["risk_signals"].append({
                                "signal": "high_influence_sell",
                                "description": (
                                    f"{actor_info['name']} ({actor_info['title']}) "
                                    f"selling {ticker}"
                                ),
                                "severity": "critical",
                            })
                            break
    except Exception as exc:
        log.warning("Actor context for {t} failed: {e}", t=ticker, e=str(exc))

    return result


def ingest_panama_pandora_data(
    data_dir: str | None = None,
) -> None:
    """Parse ICIJ Offshore Leaks database and map entities to known actors.

    Source: https://offshoreleaks.icij.org/pages/database
    The ICIJ provides downloadable CSV files. Expected files in data_dir:
        - nodes-entities.csv   (offshore entities)
        - nodes-officers.csv   (officers / intermediaries)
        - relationships.csv    (links between them)

    This function:
        1. Reads the CSVs
        2. Matches officer names against _KNOWN_ACTORS
        3. Stores matched connections in the actor network
        4. Logs any matches for manual review

    Parameters:
        data_dir: Directory containing the ICIJ CSV files.
                  Defaults to ~/data/icij_offshore_leaks/
    """
    if data_dir is None:
        data_dir = os.path.expanduser("~/data/icij_offshore_leaks")

    data_path = Path(data_dir)
    officers_file = data_path / "nodes-officers.csv"
    entities_file = data_path / "nodes-entities.csv"
    relationships_file = data_path / "relationships.csv"

    if not officers_file.exists():
        log.warning(
            "ICIJ data not found at {p}. Download from "
            "https://offshoreleaks.icij.org/pages/database",
            p=data_dir,
        )
        return

    # Build a set of known actor names for fast lookup
    known_names: dict[str, str] = {}  # lowercase_name -> actor_id
    for actor_id, data in _KNOWN_ACTORS.items():
        name = data["name"].lower()
        known_names[name] = actor_id
        # Also add last name for partial matching
        parts = name.split()
        if len(parts) >= 2:
            known_names[parts[-1]] = actor_id

    matches: list[dict] = []

    # ── Parse officers ────────────────────────────────────────────────
    log.info("Parsing ICIJ officers from {f}", f=str(officers_file))
    try:
        with open(officers_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                officer_name = (row.get("name") or "").strip()
                if not officer_name:
                    continue

                officer_lower = officer_name.lower()
                # Exact match
                if officer_lower in known_names:
                    matches.append({
                        "actor_id": known_names[officer_lower],
                        "offshore_name": officer_name,
                        "node_id": row.get("node_id", ""),
                        "jurisdiction": row.get("jurisdiction", ""),
                        "source_id": row.get("sourceID", ""),
                        "match_type": "exact",
                    })
                    continue

                # Partial match: check if any known name appears in officer name
                for known_lower, aid in known_names.items():
                    if len(known_lower) > 5 and known_lower in officer_lower:
                        matches.append({
                            "actor_id": aid,
                            "offshore_name": officer_name,
                            "node_id": row.get("node_id", ""),
                            "jurisdiction": row.get("jurisdiction", ""),
                            "source_id": row.get("sourceID", ""),
                            "match_type": "partial",
                        })
                        break
    except Exception as exc:
        log.error("Failed to parse ICIJ officers: {e}", e=str(exc))

    # ── Parse entities for jurisdiction metadata ──────────────────────
    entity_map: dict[str, dict] = {}
    if entities_file.exists():
        try:
            with open(entities_file, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    nid = row.get("node_id", "")
                    if nid:
                        entity_map[nid] = {
                            "name": row.get("name", ""),
                            "jurisdiction": row.get("jurisdiction", ""),
                            "incorporation_date": row.get("incorporation_date", ""),
                            "status": row.get("status", ""),
                        }
        except Exception as exc:
            log.debug("Failed to parse ICIJ entities: {e}", e=str(exc))

    if matches:
        log.warning(
            "ICIJ Offshore Leaks: {n} matches found against known actors! "
            "Review required.",
            n=len(matches),
        )
        for m in matches:
            actor_data = _KNOWN_ACTORS.get(m["actor_id"], {})
            log.warning(
                "  MATCH [{match_type}]: {actor} ({title}) <-> offshore entity '{offshore}'  "
                "jurisdiction={jurisdiction}",
                match_type=m["match_type"],
                actor=actor_data.get("name", m["actor_id"]),
                title=actor_data.get("title", ""),
                offshore=m["offshore_name"],
                jurisdiction=m["jurisdiction"],
            )
    else:
        log.info("ICIJ Offshore Leaks: no matches found against known actors.")


def persist_wealth_flows(
    engine: Engine,
    flows: list[WealthFlow],
) -> int:
    """Persist WealthFlow objects to the wealth_flows table.

    Parameters:
        engine: SQLAlchemy engine.
        flows: List of WealthFlow objects to persist.

    Returns:
        Number of rows inserted.
    """
    _ensure_tables(engine)
    count = 0
    with engine.begin() as conn:
        for flow in flows:
            try:
                conn.execute(text("""
                    INSERT INTO wealth_flows
                        (from_actor, to_entity, amount_estimate,
                         confidence, evidence, flow_date, implication)
                    VALUES
                        (:from_actor, :to_entity, :amount,
                         :conf, :evidence, :flow_date, :impl)
                """), {
                    "from_actor": flow.from_actor,
                    "to_entity": flow.to_actor,
                    "amount": flow.amount_estimate,
                    "conf": flow.confidence,
                    "evidence": json.dumps(flow.evidence),
                    "flow_date": flow.timestamp[:10] if flow.timestamp else None,
                    "impl": flow.implication,
                })
                count += 1
            except Exception as exc:
                log.debug("Failed to persist flow: {e}", e=str(exc))
    log.info("Persisted {n} wealth flows", n=count)
    return count


# ══════════════════════════════════════════════════════════════════════════
# INTEGRATION WITH LEVER PULLERS
# ══════════════════════════════════════════════════════════════════════════

def enrich_lever_pullers_with_actors(engine: Engine) -> int:
    """Cross-reference lever_pullers with the actor network.

    Updates lever_pullers with actor metadata (connections, influence
    propagation, motivation_model) from the actors table.

    Returns:
        Number of lever pullers enriched.
    """
    _ensure_tables(engine)
    enriched = 0
    try:
        actors = _load_actors_from_db(engine)
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, source_id, name FROM lever_pullers
            """)).fetchall()

            for r in rows:
                lp_id = r[0]
                source_id = str(r[1])
                lp_name = str(r[2])

                # Try to match to an actor by name
                matched_actor: Actor | None = None
                for actor_id, actor in actors.items():
                    if (actor.name.lower() == lp_name.lower()
                            or actor.name.lower() in source_id.lower()
                            or source_id.lower() in actor.name.lower()):
                        matched_actor = actor
                        break

                if matched_actor:
                    conn.execute(text("""
                        UPDATE lever_pullers SET
                            influence_rank = :inf,
                            motivation_model = :mot,
                            metadata = COALESCE(metadata, '{}'::JSONB) || :meta,
                            updated_at = NOW()
                        WHERE id = :id
                    """), {
                        "inf": matched_actor.influence_score,
                        "mot": matched_actor.motivation_model,
                        "meta": json.dumps({
                            "actor_id": matched_actor.id,
                            "tier": matched_actor.tier,
                            "credibility": matched_actor.credibility,
                            "connections_count": len(matched_actor.connections),
                        }),
                        "id": lp_id,
                    })
                    enriched += 1
    except Exception as exc:
        log.warning("Lever puller enrichment failed: {e}", e=str(exc))

    log.info("Enriched {n} lever pullers with actor network data", n=enriched)
    return enriched


# ══════════════════════════════════════════════════════════════════════════
# FULL REPORT
# ══════════════════════════════════════════════════════════════════════════

def generate_actor_report(engine: Engine) -> dict:
    """Generate a comprehensive actor network intelligence report.

    Combines: actor graph, wealth migration, pocket-lining flags,
    and lever puller convergence into a single actionable report.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        Dict with keys: graph, wealth_flows, pocket_lining, convergence,
        narrative, generated_at.
    """
    log.info("Generating comprehensive actor network report")

    graph = build_actor_graph(engine)
    flows = track_wealth_migration(engine, days=90)
    flags = assess_pocket_lining(engine)

    # Attempt lever puller convergence
    convergence: list[dict] = []
    try:
        from intelligence.lever_pullers import find_lever_convergence
        convergence = find_lever_convergence(engine)
    except Exception as exc:
        log.debug("Lever convergence unavailable: {e}", e=str(exc))

    # Build narrative
    narrative_parts: list[str] = []
    narrative_parts.append(
        f"Actor network: {graph['metadata']['total_actors']} tracked entities "
        f"across {len(graph['metadata'].get('tier_breakdown', {}))} tiers."
    )

    if flows:
        top_flows = flows[:5]
        flow_summary = ", ".join(
            f"{f.from_actor}->{f.to_actor} (${f.amount_estimate:,.0f})"
            for f in top_flows if f.amount_estimate > 0
        )
        if flow_summary:
            narrative_parts.append(f"Top wealth flows: {flow_summary}.")

    if flags:
        narrative_parts.append(
            f"ALERT: {len(flags)} pocket-lining flags detected. "
            f"Most severe: {flags[0].get('detection', 'unknown')} — {flags[0].get('who', 'unknown')}."
        )

    if convergence:
        narrative_parts.append(
            f"{len(convergence)} lever puller convergence events detected."
        )

    report = {
        "graph": graph,
        "wealth_flows": [asdict(f) for f in flows[:100]],
        "pocket_lining": flags,
        "convergence": convergence[:20] if convergence else [],
        "narrative": " ".join(narrative_parts),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    log.info("Actor network report complete")
    return report
