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

    # ══════════════════════════════════════════════════════════════════════
    # INSTITUTIONAL TIER — Private Credit & Shadow Banking
    # ══════════════════════════════════════════════════════════════════════
    "pc_apollo": {
        "name": "Marc Rowan / Apollo Global Management",
        "tier": "institutional",
        "category": "fund",
        "title": "CEO, Apollo Global Management ($908B AUM)",
        "net_worth_estimate": 8_200_000_000,
        "aum": 908_000_000_000,
        "influence_score": 0.93,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fee_maximization",
    },
    "pc_blackstone": {
        "name": "Steve Schwarzman / Blackstone",
        "tier": "institutional",
        "category": "fund",
        "title": "Chairman & CEO, Blackstone ($1.06T AUM)",
        "net_worth_estimate": 42_000_000_000,
        "aum": 1_065_000_000_000,
        "influence_score": 0.96,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fee_maximization",
    },
    "pc_ares": {
        "name": "Michael Arougheti / Ares Management",
        "tier": "institutional",
        "category": "fund",
        "title": "CEO, Ares Management ($428B AUM)",
        "aum": 428_000_000_000,
        "influence_score": 0.88,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fee_maximization",
    },
    "pc_kkr": {
        "name": "Scott Nuttall / KKR",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-CEO, KKR ($553B AUM)",
        "aum": 553_000_000_000,
        "influence_score": 0.91,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fee_maximization",
    },
    "pc_blue_owl": {
        "name": "Doug Ostrover / Blue Owl Capital",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-CEO, Blue Owl Capital ($235B AUM)",
        "aum": 235_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fee_maximization",
    },
    "pc_golub": {
        "name": "Lawrence Golub / Golub Capital",
        "tier": "institutional",
        "category": "fund",
        "title": "CEO, Golub Capital ($90B AUM)",
        "aum": 90_000_000_000,
        "influence_score": 0.74,
        "data_sources": ["sec_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fee_maximization",
    },
    "pc_hps": {
        "name": "Scott Kapnick / HPS Investment Partners",
        "tier": "institutional",
        "category": "fund",
        "title": "CEO, HPS Investment Partners ($117B AUM)",
        "aum": 117_000_000_000,
        "influence_score": 0.78,
        "data_sources": ["sec_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fee_maximization",
    },

    # ══════════════════════════════════════════════════════════════════════
    # INSTITUTIONAL TIER — Major Hedge Funds (private credit expansion)
    # ══════════════════════════════════════════════════════════════════════
    "hf_citadel": {
        "name": "Ken Griffin / Citadel",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder & CEO, Citadel ($65B AUM) + Citadel Securities",
        "net_worth_estimate": 45_000_000_000,
        "aum": 65_000_000_000,
        "influence_score": 0.94,
        "data_sources": ["sec_filings", "13f_filings", "finra_ats", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "multi_strategy_alpha",
    },
    "hf_bridgewater": {
        "name": "Ray Dalio / Bridgewater Associates",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Bridgewater Associates ($124B AUM)",
        "net_worth_estimate": 15_400_000_000,
        "aum": 124_000_000_000,
        "influence_score": 0.92,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "macro_fundamental",
    },
    "hf_millennium": {
        "name": "Izzy Englander / Millennium Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder & CEO, Millennium Management ($70B AUM)",
        "net_worth_estimate": 13_000_000_000,
        "aum": 70_000_000_000,
        "influence_score": 0.88,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "multi_strategy_alpha",
    },
    "hf_de_shaw": {
        "name": "David Shaw / D.E. Shaw",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, D.E. Shaw ($60B AUM, 3-and-30 fees)",
        "net_worth_estimate": 8_500_000_000,
        "aum": 60_000_000_000,
        "influence_score": 0.86,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_point72": {
        "name": "Steve Cohen / Point72",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder & CEO, Point72 ($35B AUM, ex-SAC Capital)",
        "net_worth_estimate": 21_300_000_000,
        "aum": 35_000_000_000,
        "influence_score": 0.88,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "multi_strategy_alpha",
    },
    "hf_two_sigma": {
        "name": "David Siegel & John Overdeck / Two Sigma",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-Founders, Two Sigma Investments ($75B AUM)",
        "aum": 75_000_000_000,
        "influence_score": 0.84,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_elliott": {
        "name": "Paul Singer / Elliott Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Elliott Management ($69.5B AUM, activist + distressed)",
        "net_worth_estimate": 6_200_000_000,
        "aum": 69_500_000_000,
        "influence_score": 0.90,
        "data_sources": ["sec_filings", "13f_filings", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "activist_distressed",
    },

    # ══════════════════════════════════════════════════════════════════════
    # INSTITUTIONAL TIER — Major Pension CIOs
    # ══════════════════════════════════════════════════════════════════════
    "pension_calpers_cio": {
        "name": "Stephen Gilmore",
        "tier": "institutional",
        "category": "pension_cio",
        "title": "CIO, CalPERS ($503B AUM)",
        "aum": 503_000_000_000,
        "influence_score": 0.85,
        "data_sources": ["pension_disclosures", "board_minutes", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_calstrs_cio": {
        "name": "Christopher Ailman",
        "tier": "institutional",
        "category": "pension_cio",
        "title": "CIO, CalSTRS ($340B AUM)",
        "aum": 340_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["pension_disclosures", "board_minutes", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_cppib_cio": {
        "name": "Edwin Cass",
        "tier": "institutional",
        "category": "pension_cio",
        "title": "CIO, CPP Investments ($576B CAD AUM)",
        "aum": 576_000_000_000,
        "influence_score": 0.84,
        "data_sources": ["pension_disclosures", "institutional_map"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },

    # ══════════════════════════════════════════════════════════════════════
    # BANKING DYNASTIES & FAMILIES — generational capital controllers
    # ══════════════════════════════════════════════════════════════════════
    "dynasty_rothschild": {
        "name": "Rothschild Family",
        "tier": "institutional",
        "category": "dynasty",
        "title": "Rothschild & Co, RIT Capital Partners",
        "net_worth_estimate": 20_000_000_000,
        "aum": 100_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "bank_dimon", "type": "financial_network"},
            {"actor": "am_goldman_am", "type": "historical_partnership"},
        ],
        "board_seats": ["Rothschild & Co", "RIT Capital Partners", "Jardine Matheson"],
        "political_affiliations": [
            {"party": "nonpartisan", "note": "Multi-century cross-government influence, European banking roots"},
        ],
        "known_positions": [
            {"sector": "banking", "type": "controlling_interest"},
            {"sector": "wine_estates", "type": "direct_ownership"},
            {"sector": "mining", "type": "historical_stakes"},
        ],
        "data_sources": ["sec_filings", "london_stock_exchange", "euronext", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },
    "dynasty_rockefeller": {
        "name": "Rockefeller Family",
        "tier": "institutional",
        "category": "dynasty",
        "title": "Rockefeller Capital Management, Rockefeller Foundation",
        "net_worth_estimate": 11_000_000_000,
        "aum": 45_000_000_000,
        "influence_score": 0.80,
        "connections": [
            {"actor": "am_jpmorgan_am", "type": "historical_partnership"},
            {"actor": "bank_dimon", "type": "legacy_banking"},
        ],
        "board_seats": ["Rockefeller Capital Management", "Rockefeller Foundation", "Rockefeller University"],
        "political_affiliations": [
            {"party": "bipartisan", "note": "Republican historically, now centrist philanthropy"},
        ],
        "known_positions": [
            {"sector": "real_estate", "type": "direct_ownership", "note": "Rockefeller Center"},
            {"sector": "energy", "type": "historical", "note": "Standard Oil legacy, now divested fossil fuels"},
            {"sector": "healthcare", "type": "philanthropy"},
        ],
        "data_sources": ["sec_filings", "foundation_990s", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },
    "dynasty_walton": {
        "name": "Walton Family",
        "tier": "institutional",
        "category": "dynasty",
        "title": "Walmart heirs — Jim, Rob, Alice, Lukas Walton",
        "net_worth_estimate": 267_000_000_000,
        "influence_score": 0.90,
        "connections": [
            {"actor": "am_vanguard", "type": "top_shareholder"},
        ],
        "board_seats": ["Walmart Inc.", "Walton Enterprises LLC", "Walton Family Foundation"],
        "political_affiliations": [
            {"party": "Republican", "note": "Major GOP donors, also some bipartisan giving"},
        ],
        "known_positions": [
            {"ticker": "WMT", "type": "controlling_interest", "note": "~47% of Walmart"},
            {"sector": "banking", "type": "Arvest Bank ownership"},
        ],
        "data_sources": ["sec_filings", "form4", "fec_donations", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "dynasty_koch": {
        "name": "Koch Family / Koch Industries",
        "tier": "institutional",
        "category": "dynasty",
        "title": "Charles Koch — Koch Industries, Americans for Prosperity",
        "net_worth_estimate": 128_000_000_000,
        "influence_score": 0.90,
        "connections": [
            {"actor": "pol_thiel", "type": "political_network"},
        ],
        "board_seats": ["Koch Industries", "Georgia-Pacific", "Molex", "Guardian Industries"],
        "political_affiliations": [
            {"party": "Republican", "note": "Largest conservative political donor network, Americans for Prosperity"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "direct_ownership", "note": "Refining, pipelines, chemicals"},
            {"sector": "manufacturing", "type": "direct_ownership"},
            {"sector": "technology", "type": "investments"},
        ],
        "data_sources": ["fec_donations", "opensecrets", "public_reports", "lobbying_disclosures"],
        "credibility": "public_record",
        "motivation_model": "political_influence",
    },
    "dynasty_mars": {
        "name": "Mars Family",
        "tier": "institutional",
        "category": "dynasty",
        "title": "Mars Inc. — Jacqueline, John, Victoria Mars",
        "net_worth_estimate": 160_000_000_000,
        "influence_score": 0.72,
        "board_seats": ["Mars Inc.", "Wrigley"],
        "political_affiliations": [
            {"party": "nonpartisan", "note": "Extremely private, minimal political donations"},
        ],
        "known_positions": [
            {"sector": "consumer_staples", "type": "direct_ownership", "note": "Mars, Snickers, M&M, pet food, Wrigley"},
        ],
        "data_sources": ["forbes", "private_estimates"],
        "credibility": "inferred",
        "motivation_model": "dynastic_preservation",
    },
    "dynasty_murdoch": {
        "name": "Murdoch Family",
        "tier": "institutional",
        "category": "dynasty",
        "title": "Rupert & Lachlan Murdoch — News Corp, Fox Corp",
        "net_worth_estimate": 22_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "dynasty_koch", "type": "political_alignment"},
            {"actor": "pol_thiel", "type": "media_politics_nexus"},
        ],
        "board_seats": ["Fox Corp", "News Corp", "REA Group", "Sky News Australia"],
        "political_affiliations": [
            {"party": "Republican", "note": "Fox News kingmaker, shapes GOP narratives"},
        ],
        "known_positions": [
            {"sector": "media", "type": "controlling_interest", "note": "Fox News, Wall Street Journal, NY Post, Sky"},
            {"sector": "real_estate", "type": "investments", "note": "REA Group (Australian property)"},
        ],
        "data_sources": ["sec_filings", "fec_donations", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "political_influence",
    },
    "dynasty_ambani": {
        "name": "Ambani Family",
        "tier": "institutional",
        "category": "dynasty",
        "title": "Mukesh Ambani — Chairman, Reliance Industries",
        "net_worth_estimate": 116_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "swf_adia", "type": "investment_partner"},
            {"actor": "swf_saudi_pif", "type": "investment_partner"},
            {"actor": "ind_zuckerberg", "type": "Jio_Platforms_investment"},
        ],
        "board_seats": ["Reliance Industries", "Jio Platforms", "Reliance Retail"],
        "political_affiliations": [
            {"party": "BJP", "note": "Close to Modi government, massive influence on Indian policy"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "controlling_interest", "note": "India largest refiner"},
            {"sector": "telecom", "type": "controlling_interest", "note": "Jio — 450M+ subscribers"},
            {"sector": "retail", "type": "controlling_interest", "note": "Reliance Retail"},
        ],
        "data_sources": ["bse_filings", "nse_filings", "forbes", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "empire_building",
    },
    "dynasty_adani": {
        "name": "Adani Family",
        "tier": "institutional",
        "category": "dynasty",
        "title": "Gautam Adani — Chairman, Adani Group",
        "net_worth_estimate": 84_000_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "dynasty_ambani", "type": "Indian_oligarch_peer"},
            {"actor": "swf_adia", "type": "investment_partner"},
        ],
        "board_seats": ["Adani Enterprises", "Adani Ports", "Adani Green Energy", "Adani Power", "Adani Total Gas"],
        "political_affiliations": [
            {"party": "BJP", "note": "Close Modi ally, infrastructure concessions"},
        ],
        "known_positions": [
            {"sector": "infrastructure", "type": "controlling_interest", "note": "Ports, airports, roads"},
            {"sector": "energy", "type": "controlling_interest", "note": "Coal, solar, wind"},
            {"sector": "media", "type": "controlling_interest", "note": "NDTV acquisition"},
        ],
        "data_sources": ["bse_filings", "nse_filings", "forbes", "hindenburg_report"],
        "credibility": "public_record",
        "motivation_model": "empire_building",
    },

    # ══════════════════════════════════════════════════════════════════════
    # SOVEREIGN TIER — Royal Families & Heads of State
    # ══════════════════════════════════════════════════════════════════════
    "royal_mbs": {
        "name": "Mohammed bin Salman (MBS)",
        "tier": "sovereign",
        "category": "government",
        "title": "Crown Prince & Prime Minister, Saudi Arabia",
        "net_worth_estimate": 2_000_000_000_000,  # controls sovereign wealth
        "influence_score": 0.96,
        "connections": [
            {"actor": "swf_saudi_pif", "type": "controls"},
            {"actor": "ind_musk", "type": "investment_partner"},
            {"actor": "royal_mbz", "type": "alliance"},
            {"actor": "dynasty_murdoch", "type": "media_deals"},
        ],
        "political_affiliations": [
            {"party": "Saudi_monarchy", "note": "Absolute ruler, Vision 2030 architect"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "sovereign_control", "note": "Saudi Aramco, OPEC+ leader"},
            {"sector": "sports", "type": "investments", "note": "Newcastle United, LIV Golf, WWE"},
            {"sector": "technology", "type": "investments", "note": "SoftBank, Lucid Motors, Jio"},
        ],
        "data_sources": ["swf_reports", "13f_filings", "opec_decisions", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "geopolitical",
    },
    "royal_mbz": {
        "name": "Sheikh Mohammed bin Zayed (MBZ)",
        "tier": "sovereign",
        "category": "government",
        "title": "President, United Arab Emirates",
        "influence_score": 0.94,
        "connections": [
            {"actor": "swf_adia", "type": "controls"},
            {"actor": "swf_mubadala", "type": "controls"},
            {"actor": "royal_mbs", "type": "alliance"},
        ],
        "political_affiliations": [
            {"party": "UAE_monarchy", "note": "Controls ADIA, Mubadala, ADQ — combined $1.5T+"},
        ],
        "known_positions": [
            {"sector": "technology", "type": "investments", "note": "G42 AI, sovereign AI compute"},
            {"sector": "defense", "type": "investments"},
            {"sector": "real_estate", "type": "sovereign_control"},
        ],
        "data_sources": ["swf_reports", "13f_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "geopolitical",
    },
    "royal_qatar_tamim": {
        "name": "Sheikh Tamim bin Hamad Al Thani",
        "tier": "sovereign",
        "category": "government",
        "title": "Emir, State of Qatar",
        "influence_score": 0.86,
        "connections": [
            {"actor": "swf_qatar_qia", "type": "controls"},
        ],
        "political_affiliations": [
            {"party": "Qatari_monarchy", "note": "Controls QIA, Al Jazeera, major global real estate"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "sovereign_control", "note": "World's largest LNG exporter"},
            {"sector": "real_estate", "type": "investments", "note": "London Shard, Harrods, Canary Wharf"},
            {"sector": "sports", "type": "investments", "note": "PSG, 2022 World Cup"},
        ],
        "data_sources": ["swf_reports", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "geopolitical",
    },
    "royal_brunei": {
        "name": "Sultan Hassanal Bolkiah",
        "tier": "sovereign",
        "category": "government",
        "title": "Sultan of Brunei",
        "net_worth_estimate": 30_000_000_000,
        "influence_score": 0.58,
        "connections": [],
        "political_affiliations": [
            {"party": "Brunei_monarchy", "note": "Absolute monarch, BIA sovereign wealth"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "sovereign_control", "note": "Oil & gas revenues"},
            {"sector": "hospitality", "type": "direct_ownership", "note": "Dorchester Collection hotels"},
        ],
        "data_sources": ["public_reports", "forbes"],
        "credibility": "inferred",
        "motivation_model": "dynastic_preservation",
    },

    # ══════════════════════════════════════════════════════════════════════
    # SOVEREIGN WEALTH FUNDS — additional to existing SWFs
    # ══════════════════════════════════════════════════════════════════════
    "swf_mubadala": {
        "name": "Mubadala Investment Company",
        "tier": "institutional",
        "category": "swf",
        "title": "Mubadala — Abu Dhabi Sovereign Wealth Fund",
        "aum": 302_000_000_000,
        "influence_score": 0.80,
        "connections": [
            {"actor": "royal_mbz", "type": "controlled_by"},
            {"actor": "swf_adia", "type": "peer_fund"},
        ],
        "known_positions": [
            {"sector": "technology", "type": "investments", "note": "GlobalFoundries, AMD stake"},
            {"sector": "aerospace", "type": "investments", "note": "Strata Manufacturing"},
            {"sector": "healthcare", "type": "investments", "note": "Cleveland Clinic Abu Dhabi"},
        ],
        "data_sources": ["13f_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "swf_adq": {
        "name": "ADQ (Abu Dhabi Developmental Holding)",
        "tier": "institutional",
        "category": "swf",
        "title": "ADQ — Abu Dhabi State Holding Company",
        "aum": 157_000_000_000,
        "influence_score": 0.72,
        "connections": [
            {"actor": "royal_mbz", "type": "controlled_by"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "swf_adic": {
        "name": "Abu Dhabi Investment Council (ADIC)",
        "tier": "institutional",
        "category": "swf",
        "title": "ADIC — Abu Dhabi Investment Council",
        "aum": 110_000_000_000,
        "influence_score": 0.68,
        "connections": [
            {"actor": "royal_mbz", "type": "controlled_by"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "inferred",
        "motivation_model": "institutional_mandate",
    },
    "swf_safe_china": {
        "name": "State Administration of Foreign Exchange (SAFE)",
        "tier": "sovereign",
        "category": "swf",
        "title": "SAFE Investment Company — China FX Reserves Manager",
        "aum": 1_000_000_000_000,
        "influence_score": 0.90,
        "connections": [
            {"actor": "pboc_pan", "type": "operates_under"},
            {"actor": "swf_cic", "type": "peer_fund"},
        ],
        "data_sources": ["pboc_reports", "imf_cofer"],
        "credibility": "inferred",
        "motivation_model": "geopolitical",
    },
    "swf_gpif_japan": {
        "name": "Government Pension Investment Fund (GPIF)",
        "tier": "institutional",
        "category": "swf",
        "title": "GPIF — World's Largest Pension Fund (Japan)",
        "aum": 1_600_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "boj_ueda", "type": "policy_coordination"},
        ],
        "known_positions": [
            {"sector": "global_equities", "type": "index_allocation", "note": "25% domestic, 25% foreign equities"},
            {"sector": "bonds", "type": "index_allocation", "note": "25% domestic, 25% foreign bonds"},
        ],
        "data_sources": ["gpif_reports", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "swf_nps_korea": {
        "name": "National Pension Service (NPS) — South Korea",
        "tier": "institutional",
        "category": "swf",
        "title": "NPS — South Korea National Pension Fund",
        "aum": 900_000_000_000,
        "influence_score": 0.80,
        "connections": [],
        "known_positions": [
            {"sector": "korean_equities", "type": "major_shareholder", "note": "Top holder in Samsung, SK Hynix"},
            {"sector": "global_equities", "type": "index_allocation"},
        ],
        "data_sources": ["nps_reports", "13f_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ══════════════════════════════════════════════════════════════════════
    # INSTITUTIONAL TIER — Bank CEOs (additional)
    # ══════════════════════════════════════════════════════════════════════
    "bank_waldron": {
        "name": "John Waldron",
        "tier": "institutional",
        "category": "corporation",
        "title": "President & COO, Goldman Sachs",
        "influence_score": 0.76,
        "connections": [
            {"actor": "bank_solomon", "type": "same_firm"},
        ],
        "data_sources": ["sec_filings", "earnings_calls"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "bank_gorman": {
        "name": "James Gorman",
        "tier": "institutional",
        "category": "corporation",
        "title": "Executive Chairman, Morgan Stanley",
        "influence_score": 0.78,
        "data_sources": ["sec_filings", "earnings_calls"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "bank_fraser": {
        "name": "Jane Fraser",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, Citigroup",
        "influence_score": 0.76,
        "data_sources": ["sec_filings", "earnings_calls"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "bank_scharf": {
        "name": "Charles Scharf",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, Wells Fargo",
        "influence_score": 0.74,
        "data_sources": ["sec_filings", "earnings_calls"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ══════════════════════════════════════════════════════════════════════
    # MEGA FUND MANAGERS — additional (some already exist above)
    # ══════════════════════════════════════════════════════════════════════
    "hf_yass": {
        "name": "Jeff Yass",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-Founder, Susquehanna International Group (SIG)",
        "net_worth_estimate": 42_000_000_000,
        "aum": 75_000_000_000,
        "influence_score": 0.86,
        "connections": [
            {"actor": "pol_thiel", "type": "political_donor_peer"},
        ],
        "political_affiliations": [
            {"party": "Republican/Libertarian", "note": "Top GOP donor, TikTok investor (ByteDance), Club for Growth"},
        ],
        "known_positions": [
            {"sector": "options_market_making", "type": "direct_ownership"},
            {"ticker": "BABA", "type": "large_position"},
            {"ticker": "BIDU", "type": "large_position"},
        ],
        "data_sources": ["13f_filings", "sec_edgar", "fec_donations", "options_flow"],
        "credibility": "hard_data",
        "motivation_model": "quantitative",
    },
    "hf_coleman_tiger": {
        "name": "Chase Coleman III",
        "tier": "institutional",
        "category": "fund",
        "title": "Founder, Tiger Global Management",
        "net_worth_estimate": 8_500_000_000,
        "aum": 75_000_000_000,
        "influence_score": 0.78,
        "connections": [
            {"actor": "hf_tiger_global", "type": "same_entity"},
        ],
        "known_positions": [
            {"sector": "technology", "type": "concentrated", "note": "Tech/growth focused"},
        ],
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "growth_conviction",
    },

    # ══════════════════════════════════════════════════════════════════════
    # SOVEREIGN TIER — Additional Central Bank Governors
    # ══════════════════════════════════════════════════════════════════════
    "rba_bullock": {
        "name": "Michele Bullock",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Reserve Bank of Australia",
        "influence_score": 0.78,
        "data_sources": ["rba_decisions", "rba_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "boc_macklem": {
        "name": "Tiff Macklem",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Bank of Canada",
        "influence_score": 0.80,
        "data_sources": ["boc_decisions", "boc_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "boj_kuroda_legacy": {
        "name": "Haruhiko Kuroda (Legacy)",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Former Governor, Bank of Japan (2013-2023)",
        "influence_score": 0.70,
        "connections": [
            {"actor": "boj_ueda", "type": "predecessor"},
        ],
        "data_sources": ["boj_historical", "academic"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "snb_jordan": {
        "name": "Thomas Jordan",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Chairman, Swiss National Bank",
        "influence_score": 0.80,
        "known_positions": [
            {"sector": "equities", "type": "fx_reserves_allocation", "note": "SNB holds $170B+ in US equities"},
        ],
        "data_sources": ["snb_reports", "13f_filings"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ══════════════════════════════════════════════════════════════════════
    # INDIVIDUAL TIER — Tech Power Players
    # ══════════════════════════════════════════════════════════════════════
    "ind_page": {
        "name": "Larry Page",
        "tier": "individual",
        "category": "insider",
        "title": "Co-Founder, Alphabet/Google",
        "net_worth_estimate": 156_000_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "ind_brin", "type": "co_founder"},
        ],
        "known_positions": [
            {"ticker": "GOOGL", "type": "controlling_interest", "note": "Class B supervoting shares"},
        ],
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "diversification",
    },
    "ind_brin": {
        "name": "Sergey Brin",
        "tier": "individual",
        "category": "insider",
        "title": "Co-Founder, Alphabet/Google",
        "net_worth_estimate": 148_000_000_000,
        "influence_score": 0.80,
        "connections": [
            {"actor": "ind_page", "type": "co_founder"},
        ],
        "known_positions": [
            {"ticker": "GOOGL", "type": "controlling_interest", "note": "Class B supervoting shares"},
        ],
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "diversification",
    },

    # ══════════════════════════════════════════════════════════════════════
    # INDIVIDUAL/INSTITUTIONAL — Political Kingmakers & Mega-Donors
    # ══════════════════════════════════════════════════════════════════════
    "pol_thiel": {
        "name": "Peter Thiel",
        "tier": "individual",
        "category": "kingmaker",
        "title": "Co-Founder, Palantir & PayPal; Founders Fund",
        "net_worth_estimate": 11_000_000_000,
        "aum": 12_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "ind_musk", "type": "PayPal_mafia"},
            {"actor": "dynasty_koch", "type": "political_alignment"},
            {"actor": "pol_andreessen", "type": "vc_politics_nexus"},
        ],
        "board_seats": ["Palantir Technologies"],
        "political_affiliations": [
            {"party": "Republican", "note": "Backed JD Vance for Senate/VP, Blake Masters, major GOP tech donor"},
        ],
        "known_positions": [
            {"ticker": "PLTR", "type": "co_founder_stake"},
            {"sector": "defense_tech", "type": "investments", "note": "Anduril, Palantir govt contracts"},
            {"sector": "crypto", "type": "investments", "note": "Early Bitcoin, Bullish exchange"},
        ],
        "data_sources": ["sec_filings", "fec_donations", "opensecrets", "form4"],
        "credibility": "hard_data",
        "motivation_model": "political_influence",
    },
    "pol_soros_george": {
        "name": "George Soros (Political)",
        "tier": "individual",
        "category": "kingmaker",
        "title": "Founder, Open Society Foundations",
        "net_worth_estimate": 6_700_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "hf_soros", "type": "same_entity"},
            {"actor": "pol_hoffman", "type": "democratic_donor_peer"},
        ],
        "political_affiliations": [
            {"party": "Democratic", "note": "Largest Democratic mega-donor, Open Society $32B+ deployed, DA races"},
        ],
        "known_positions": [
            {"sector": "philanthropy", "type": "direct", "note": "Open Society Foundations — 120+ countries"},
        ],
        "data_sources": ["fec_donations", "opensecrets", "foundation_990s"],
        "credibility": "public_record",
        "motivation_model": "political_influence",
    },
    "pol_bloomberg": {
        "name": "Michael Bloomberg",
        "tier": "individual",
        "category": "kingmaker",
        "title": "Founder, Bloomberg LP; former NYC Mayor",
        "net_worth_estimate": 106_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "pol_soros_george", "type": "democratic_donor_peer"},
        ],
        "board_seats": ["Bloomberg LP", "Bloomberg Philanthropies"],
        "political_affiliations": [
            {"party": "Democratic/Independent", "note": "Gun control, climate funding, 2020 presidential run"},
        ],
        "known_positions": [
            {"sector": "media", "type": "controlling_interest", "note": "Bloomberg terminal monopoly, Bloomberg News"},
            {"sector": "finance_data", "type": "controlling_interest", "note": "Bloomberg Terminal — $12B+ revenue"},
        ],
        "data_sources": ["fec_donations", "opensecrets", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "political_influence",
    },
    "pol_hoffman": {
        "name": "Reid Hoffman",
        "tier": "individual",
        "category": "kingmaker",
        "title": "Co-Founder, LinkedIn; Partner, Greylock",
        "net_worth_estimate": 2_500_000_000,
        "influence_score": 0.74,
        "connections": [
            {"actor": "pol_soros_george", "type": "democratic_donor_peer"},
            {"actor": "ind_altman", "type": "openai_board_member"},
            {"actor": "pol_andreessen", "type": "vc_rival"},
        ],
        "political_affiliations": [
            {"party": "Democratic", "note": "Major Democratic tech donor, AI policy influence"},
        ],
        "known_positions": [
            {"ticker": "MSFT", "type": "former_board", "note": "LinkedIn acquired by Microsoft"},
            {"sector": "ai", "type": "investments", "note": "OpenAI early backer, Inflection AI"},
        ],
        "data_sources": ["sec_filings", "fec_donations", "opensecrets"],
        "credibility": "public_record",
        "motivation_model": "political_influence",
    },
    "pol_andreessen": {
        "name": "Marc Andreessen",
        "tier": "individual",
        "category": "kingmaker",
        "title": "Co-Founder, Andreessen Horowitz (a16z)",
        "net_worth_estimate": 1_900_000_000,
        "aum": 42_000_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "pol_thiel", "type": "vc_politics_nexus"},
            {"actor": "ind_musk", "type": "tech_politics_nexus"},
        ],
        "political_affiliations": [
            {"party": "Republican (recent shift)", "note": "Endorsed Trump 2024, anti-regulation, pro-crypto"},
        ],
        "known_positions": [
            {"sector": "crypto", "type": "investments", "note": "a16z crypto fund, major Web3 backer"},
            {"sector": "ai", "type": "investments", "note": "Mistral, Character.ai, Databricks"},
            {"sector": "fintech", "type": "investments", "note": "Coinbase, Stripe"},
        ],
        "data_sources": ["sec_filings", "fec_donations", "opensecrets", "crunchbase"],
        "credibility": "public_record",
        "motivation_model": "political_influence",
    },
    "pol_sbf_legacy": {
        "name": "Sam Bankman-Fried (Cautionary Legacy)",
        "tier": "individual",
        "category": "cautionary",
        "title": "Former CEO, FTX — convicted of fraud",
        "net_worth_estimate": 0,
        "influence_score": 0.40,
        "political_affiliations": [
            {"party": "Democratic", "note": "Second-largest Dem donor 2022 cycle, $40M+, all fraudulent funds"},
        ],
        "known_positions": [
            {"sector": "crypto", "type": "bankrupt", "note": "FTX collapse Nov 2022, $8B customer funds lost"},
        ],
        "data_sources": ["court_filings", "fec_donations"],
        "credibility": "hard_data",
        "motivation_model": "cautionary_fraud",
    },

    # ══════════════════════════════════════════════════════════════════════
    # INDIVIDUAL TIER — Tech Power (additional to existing)
    # ══════════════════════════════════════════════════════════════════════
    "ind_musk_expanded": {
        "name": "Elon Musk (Expanded Profile)",
        "tier": "individual",
        "category": "insider",
        "title": "CEO, Tesla & SpaceX; Founder, xAI; Head, DOGE",
        "net_worth_estimate": 240_000_000_000,
        "influence_score": 0.97,
        "connections": [
            {"actor": "pol_thiel", "type": "PayPal_mafia"},
            {"actor": "royal_mbs", "type": "investment_partner"},
            {"actor": "ind_altman", "type": "ai_rival"},
        ],
        "political_affiliations": [
            {"party": "Republican", "note": "Major Trump backer, DOGE government role, $250M+ political spending"},
        ],
        "known_positions": [
            {"ticker": "TSLA", "type": "controlling_interest", "note": "~13% stake, CEO"},
            {"sector": "aerospace", "type": "direct_ownership", "note": "SpaceX — $210B valuation"},
            {"sector": "ai", "type": "direct_ownership", "note": "xAI — Grok, $50B+ valuation"},
            {"sector": "social_media", "type": "direct_ownership", "note": "X/Twitter"},
            {"sector": "neurotechnology", "type": "direct_ownership", "note": "Neuralink"},
            {"sector": "infrastructure", "type": "direct_ownership", "note": "Boring Company"},
        ],
        "data_sources": ["sec_filings", "form4", "social_media", "fec_donations", "government_contracts"],
        "credibility": "hard_data",
        "motivation_model": "empire_building",
    },
    "ind_bezos_expanded": {
        "name": "Jeff Bezos (Expanded Profile)",
        "tier": "individual",
        "category": "insider",
        "title": "Founder & Executive Chairman, Amazon; Owner, Washington Post",
        "net_worth_estimate": 215_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "ind_jassy", "type": "appointed_ceo"},
            {"actor": "pol_bloomberg", "type": "media_owner_peer"},
        ],
        "known_positions": [
            {"ticker": "AMZN", "type": "founder_stake", "note": "~9% of Amazon"},
            {"sector": "aerospace", "type": "direct_ownership", "note": "Blue Origin"},
            {"sector": "media", "type": "direct_ownership", "note": "Washington Post"},
            {"sector": "real_estate", "type": "investments", "note": "Bezos Expeditions portfolio"},
        ],
        "data_sources": ["sec_filings", "form4", "bezos_expeditions"],
        "credibility": "hard_data",
        "motivation_model": "diversification",
    },
    "ind_ellison": {
        "name": "Larry Ellison",
        "tier": "individual",
        "category": "insider",
        "title": "Co-Founder, CTO & Chairman, Oracle",
        "net_worth_estimate": 200_000_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "ind_musk", "type": "personal_friendship_tesla_board"},
        ],
        "board_seats": ["Oracle Corp", "Tesla Inc (former)"],
        "known_positions": [
            {"ticker": "ORCL", "type": "controlling_interest", "note": "~40% of Oracle"},
            {"ticker": "TSLA", "type": "large_position"},
            {"sector": "real_estate", "type": "direct_ownership", "note": "98% of Lanai island, Hawaii"},
        ],
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "empire_building",
    },
    "ind_gates": {
        "name": "Bill Gates",
        "tier": "individual",
        "category": "insider",
        "title": "Co-Founder, Microsoft; Co-Chair, Gates Foundation",
        "net_worth_estimate": 130_000_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "ind_nadella", "type": "appointed_ceo"},
            {"actor": "ind_buffett", "type": "giving_pledge_partner"},
        ],
        "board_seats": ["Cascade Investment LLC", "Gates Foundation"],
        "known_positions": [
            {"ticker": "MSFT", "type": "founder_stake", "note": "Reduced to ~1.4%"},
            {"sector": "farmland", "type": "direct_ownership", "note": "Largest US farmland owner — 270K+ acres"},
            {"ticker": "BRK.B", "type": "large_position"},
            {"ticker": "WM", "type": "large_position", "note": "Via Cascade Investment"},
            {"ticker": "CNI", "type": "large_position"},
        ],
        "data_sources": ["sec_filings", "form4", "13f_filings", "foundation_990s"],
        "credibility": "hard_data",
        "motivation_model": "philanthropy_diversification",
    },
    "ind_zuckerberg_expanded": {
        "name": "Mark Zuckerberg (Expanded Profile)",
        "tier": "individual",
        "category": "insider",
        "title": "Founder, Chairman & CEO, Meta Platforms",
        "net_worth_estimate": 185_000_000_000,
        "influence_score": 0.86,
        "connections": [
            {"actor": "dynasty_ambani", "type": "Jio_investment"},
            {"actor": "pol_thiel", "type": "early_facebook_investor"},
        ],
        "known_positions": [
            {"ticker": "META", "type": "controlling_interest", "note": "Class B supervoting shares, ~13% economic"},
            {"sector": "ai", "type": "direct_investment", "note": "LLaMA models, $40B+ AI capex"},
            {"sector": "vr_ar", "type": "direct_investment", "note": "Reality Labs, Quest"},
        ],
        "data_sources": ["sec_filings", "form4"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ind_nadella_expanded": {
        "name": "Satya Nadella (Expanded Profile)",
        "tier": "individual",
        "category": "insider",
        "title": "Chairman & CEO, Microsoft",
        "net_worth_estimate": 1_200_000_000,
        "influence_score": 0.86,
        "connections": [
            {"actor": "ind_altman", "type": "openai_partnership"},
            {"actor": "ind_gates", "type": "microsoft_founder"},
        ],
        "known_positions": [
            {"ticker": "MSFT", "type": "ceo_stake"},
            {"sector": "ai", "type": "partnership", "note": "OpenAI — $13B+ invested, exclusive Azure deal"},
            {"sector": "gaming", "type": "direct_ownership", "note": "Activision Blizzard $69B acquisition"},
        ],
        "data_sources": ["sec_filings", "form4", "earnings_calls"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ind_altman_expanded": {
        "name": "Sam Altman (Expanded Profile)",
        "tier": "individual",
        "category": "insider",
        "title": "CEO, OpenAI — most powerful figure in AI",
        "net_worth_estimate": 2_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "ind_nadella_expanded", "type": "microsoft_partnership"},
            {"actor": "pol_hoffman", "type": "openai_board"},
            {"actor": "ind_musk_expanded", "type": "ai_rival_former_cofounder"},
        ],
        "known_positions": [
            {"sector": "ai", "type": "ceo", "note": "OpenAI — GPT, $150B+ valuation"},
            {"sector": "nuclear", "type": "investments", "note": "Helion Energy, Oklo"},
            {"sector": "crypto", "type": "investments", "note": "Worldcoin/World"},
        ],
        "data_sources": ["public_statements", "opensecrets", "crunchbase"],
        "credibility": "public_record",
        "motivation_model": "empire_building",
    },
    "ind_jensen_expanded": {
        "name": "Jensen Huang (Expanded Profile)",
        "tier": "individual",
        "category": "insider",
        "title": "Founder & CEO, NVIDIA — AI kingmaker",
        "net_worth_estimate": 120_000_000_000,
        "influence_score": 0.92,
        "connections": [
            {"actor": "ind_altman_expanded", "type": "supplier"},
            {"actor": "ind_nadella_expanded", "type": "supplier"},
            {"actor": "ind_zuckerberg_expanded", "type": "supplier"},
        ],
        "known_positions": [
            {"ticker": "NVDA", "type": "founder_stake", "note": "~3.5%, CEO — controls AI chip supply"},
        ],
        "data_sources": ["sec_filings", "form4", "earnings_calls"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ══════════════════════════════════════════════════════════════════════
    # INSTITUTIONAL — Additional Major Players
    # ══════════════════════════════════════════════════════════════════════
    "pe_schwarzman": {
        "name": "Stephen Schwarzman",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-Founder, Chairman & CEO, Blackstone",
        "net_worth_estimate": 42_000_000_000,
        "aum": 1_000_000_000_000,
        "influence_score": 0.90,
        "connections": [
            {"actor": "am_fink", "type": "peer"},
            {"actor": "dynasty_koch", "type": "political_alignment"},
        ],
        "political_affiliations": [
            {"party": "Republican", "note": "Major GOP donor, Trump advisory council"},
        ],
        "known_positions": [
            {"sector": "real_estate", "type": "largest_owner", "note": "Largest commercial RE owner globally"},
            {"sector": "private_equity", "type": "controlling_interest"},
            {"sector": "credit", "type": "major_player"},
        ],
        "data_sources": ["sec_filings", "13f_filings", "fec_donations"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "pe_kravis": {
        "name": "Henry Kravis / KKR",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-Founder, KKR & Co.",
        "net_worth_estimate": 11_000_000_000,
        "aum": 553_000_000_000,
        "influence_score": 0.82,
        "data_sources": ["sec_filings", "13f_filings"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "pe_rubenstein": {
        "name": "David Rubenstein / Carlyle Group",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-Founder, Carlyle Group",
        "net_worth_estimate": 4_600_000_000,
        "aum": 426_000_000_000,
        "influence_score": 0.80,
        "connections": [
            {"actor": "pe_schwarzman", "type": "pe_peer"},
        ],
        "data_sources": ["sec_filings", "13f_filings"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "pe_apollo_leon_black": {
        "name": "Marc Rowan / Apollo Global Management",
        "tier": "institutional",
        "category": "fund",
        "title": "CEO, Apollo Global Management",
        "net_worth_estimate": 6_000_000_000,
        "aum": 671_000_000_000,
        "influence_score": 0.84,
        "data_sources": ["sec_filings", "13f_filings"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "am_oaktree_marks": {
        "name": "Oaktree Capital Management",
        "tier": "institutional",
        "category": "fund",
        "title": "Oaktree Capital — Distressed Debt King",
        "aum": 189_000_000_000,
        "influence_score": 0.80,
        "connections": [
            {"actor": "ind_marks", "type": "founder"},
        ],
        "data_sources": ["13f_filings", "sec_edgar"],
        "credibility": "hard_data",
        "motivation_model": "value_investor",
    },

    # ══════════════════════════════════════════════════════════════════════
    # CONNECTIONS MAP — cross-references for graph building
    # Known alliances, rivalries, and capital flow channels
    # ══════════════════════════════════════════════════════════════════════
    # NOTE: The connections fields above encode directional relationships.
    # The graph builder (build_actor_graph) treats these bidirectionally.
    # Key relationship types:
    #   - controls: SWF/royal controls the fund
    #   - same_entity: different records for same org
    #   - investment_partner: co-invest or LP relationship
    #   - political_alignment: shared political goals
    #   - co_founder: co-founded same company
    #   - peer: same tier/category competitor
    #   - supplier: B2B supply chain relationship
    #   - alliance: geopolitical alliance
}

# Confirm count at module-load time for development
_ACTOR_COUNT = len(_KNOWN_ACTORS)
assert _ACTOR_COUNT >= 150, (
    f"Expected >= 150 known actors, got {_ACTOR_COUNT}. Add more seed data."
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
