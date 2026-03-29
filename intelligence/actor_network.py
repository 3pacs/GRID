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
    # GLOBAL EXPANSION — ASIA
    # ══════════════════════════════════════════════════════════════════════

    # ── Japan ──────────────────────────────────────────────────────────
    "jp_masayoshi_son": {
        "name": "Masayoshi Son",
        "tier": "institutional",
        "category": "corporation",
        "title": "Founder & CEO, SoftBank Group",
        "net_worth_estimate": 23_000_000_000,
        "aum": 100_000_000_000,  # Vision Fund
        "influence_score": 0.90,
        "connections": [
            {"actor": "royal_mbs", "type": "investment_partner"},
            {"actor": "swf_saudi_pif", "type": "LP_relationship"},
            {"actor": "cn_jack_ma", "type": "early_investor"},
            {"actor": "ind_musk", "type": "investment_partner"},
        ],
        "political_affiliations": [
            {"party": "independent", "note": "Close to Japanese PM, Saudi crown prince; Vision Fund reshaped global VC"},
        ],
        "board_seats": ["SoftBank Group", "Arm Holdings", "T-Mobile (via Sprint legacy)"],
        "known_positions": [
            {"sector": "technology", "type": "controlling_stake", "note": "Arm Holdings, SoftBank Vision Fund I & II ($100B+)"},
            {"sector": "telecom", "type": "controlling_stake", "note": "SoftBank Corp (Japan #3 carrier)"},
            {"sector": "AI", "type": "investments", "note": "Massive AI bets post-2024: $100B Stargate, Arm AI chips"},
        ],
        "data_sources": ["sec_filings", "tse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "visionary_tech",
    },
    "jp_akio_toyoda": {
        "name": "Akio Toyoda",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, Toyota Motor Corporation",
        "net_worth_estimate": 1_200_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "jp_denso", "type": "supplier"},
        ],
        "political_affiliations": [
            {"party": "Keidanren", "note": "Toyota family dynasty; Japan's largest manufacturer; Japan Auto Manufacturers Assoc chairman"},
        ],
        "board_seats": ["Toyota Motor Corp"],
        "known_positions": [
            {"sector": "automotive", "type": "controlling_family", "note": "Toyota: $300B+ market cap, world's largest automaker"},
            {"sector": "hydrogen", "type": "strategic_bet", "note": "Pushed hydrogen over full EV"},
            {"sector": "AI_robotics", "type": "investments", "note": "Woven Planet, Toyota Research Institute"},
        ],
        "data_sources": ["tse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "jp_tadashi_yanai": {
        "name": "Tadashi Yanai",
        "tier": "institutional",
        "category": "corporation",
        "title": "Founder & Chairman, Fast Retailing (Uniqlo)",
        "net_worth_estimate": 38_000_000_000,
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Japan's richest person; vocal on Japan economic reform"},
        ],
        "known_positions": [
            {"sector": "retail", "type": "controlling_stake", "note": "Fast Retailing (Uniqlo): global fashion empire"},
        ],
        "data_sources": ["tse_filings", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "profit_maximizer",
    },
    "jp_ken_miyauchi": {
        "name": "Ken Miyauchi",
        "tier": "institutional",
        "category": "corporation",
        "title": "Former CEO, SoftBank Corp (domestic telecom)",
        "influence_score": 0.55,
        "connections": [
            {"actor": "jp_masayoshi_son", "type": "same_entity"},
        ],
        "data_sources": ["tse_filings"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "jp_boj_deputy_himino": {
        "name": "Ryozo Himino",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Deputy Governor, Bank of Japan",
        "influence_score": 0.80,
        "connections": [
            {"actor": "boj_ueda", "type": "institutional_peer"},
        ],
        "data_sources": ["boj_decisions", "boj_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ── South Korea (Chaebols) ─────────────────────────────────────────
    "kr_lee_jaeyong": {
        "name": "Lee Jae-yong",
        "tier": "institutional",
        "category": "corporation",
        "title": "Executive Chairman, Samsung Electronics",
        "net_worth_estimate": 11_000_000_000,
        "influence_score": 0.90,
        "connections": [
            {"actor": "kr_samsung_group", "type": "controls"},
        ],
        "political_affiliations": [
            {"party": "Samsung_dynasty", "note": "Convicted/pardoned for bribery (Park Geun-hye scandal); Samsung = ~20% of Korea GDP"},
        ],
        "known_positions": [
            {"sector": "semiconductors", "type": "controlling_family", "note": "Samsung: world's largest memory chip maker"},
            {"sector": "displays", "type": "controlling_family", "note": "Samsung Display: OLED monopoly"},
            {"sector": "insurance", "type": "controlling_family", "note": "Samsung Life, Samsung Fire"},
            {"sector": "construction", "type": "controlling_family", "note": "Samsung C&T, Samsung Engineering"},
        ],
        "data_sources": ["krx_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "kr_samsung_group": {
        "name": "Samsung Group",
        "tier": "institutional",
        "category": "corporation",
        "title": "Samsung Group — Korea's Largest Chaebol",
        "aum": 500_000_000_000,  # combined group market cap
        "influence_score": 0.92,
        "connections": [
            {"actor": "kr_lee_jaeyong", "type": "controlled_by"},
        ],
        "known_positions": [
            {"sector": "semiconductors", "type": "dominant", "note": "Memory chips, foundry, displays, phones, insurance, shipbuilding"},
        ],
        "data_sources": ["krx_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "kr_chung_euisun": {
        "name": "Chung Euisun",
        "tier": "institutional",
        "category": "corporation",
        "title": "Executive Chairman, Hyundai Motor Group",
        "net_worth_estimate": 8_000_000_000,
        "influence_score": 0.78,
        "connections": [],
        "political_affiliations": [
            {"party": "Hyundai_dynasty", "note": "Chung family controls Hyundai Motor, Kia, Hyundai Steel, HD Hyundai (shipbuilding)"},
        ],
        "known_positions": [
            {"sector": "automotive", "type": "controlling_family", "note": "Hyundai + Kia = #3 global automaker"},
            {"sector": "robotics", "type": "investments", "note": "Boston Dynamics acquisition"},
            {"sector": "steel", "type": "controlling_family"},
            {"sector": "shipbuilding", "type": "controlling_family", "note": "HD Hyundai: world's largest shipbuilder"},
        ],
        "data_sources": ["krx_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "kr_sk_chey_taewon": {
        "name": "Chey Tae-won",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, SK Group",
        "net_worth_estimate": 5_500_000_000,
        "influence_score": 0.76,
        "connections": [],
        "political_affiliations": [
            {"party": "SK_dynasty", "note": "SK Group: semiconductors (SK hynix), energy, telecom, pharma"},
        ],
        "known_positions": [
            {"sector": "semiconductors", "type": "controlling_family", "note": "SK hynix: #2 global memory chipmaker"},
            {"sector": "EV_batteries", "type": "controlling_family", "note": "SK On: top-5 EV battery maker"},
            {"sector": "telecom", "type": "controlling_family", "note": "SK Telecom: Korea's #1 carrier"},
        ],
        "data_sources": ["krx_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "kr_lg_koo_kwangmo": {
        "name": "Koo Kwang-mo",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, LG Group",
        "net_worth_estimate": 2_500_000_000,
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "LG_dynasty", "note": "LG Group: electronics, chemicals, EV batteries (LG Energy Solution)"},
        ],
        "known_positions": [
            {"sector": "EV_batteries", "type": "controlling_family", "note": "LG Energy Solution: #2 global EV battery maker"},
            {"sector": "chemicals", "type": "controlling_family", "note": "LG Chem"},
            {"sector": "electronics", "type": "controlling_family"},
        ],
        "data_sources": ["krx_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "kr_bok_rhee": {
        "name": "Rhee Chang-yong",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Bank of Korea",
        "influence_score": 0.78,
        "connections": [],
        "data_sources": ["bok_decisions", "bok_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ── India ──────────────────────────────────────────────────────────
    "in_mukesh_ambani": {
        "name": "Mukesh Ambani",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman & MD, Reliance Industries",
        "net_worth_estimate": 116_000_000_000,
        "influence_score": 0.92,
        "connections": [
            {"actor": "royal_mbs", "type": "investment_partner"},
            {"actor": "ind_zuckerberg", "type": "investment_partner"},
            {"actor": "ind_pichai", "type": "investment_partner"},
        ],
        "political_affiliations": [
            {"party": "BJP_aligned", "note": "Close to PM Modi; Reliance controls ~5% of India GDP; Jio transformed India's digital economy"},
        ],
        "known_positions": [
            {"sector": "telecom", "type": "controlling_stake", "note": "Jio: India's #1 carrier, 450M+ subscribers"},
            {"sector": "energy", "type": "controlling_stake", "note": "Reliance: India's largest private company, refinery complex"},
            {"sector": "retail", "type": "controlling_stake", "note": "Reliance Retail: India's largest retailer"},
            {"sector": "media", "type": "controlling_stake", "note": "Viacom18, JioCinema, Network18"},
            {"sector": "technology", "type": "investments", "note": "Jio Platforms: Google, Meta, Intel invested"},
        ],
        "data_sources": ["bse_filings", "nse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "empire_builder",
    },
    "in_gautam_adani": {
        "name": "Gautam Adani",
        "tier": "institutional",
        "category": "corporation",
        "title": "Founder & Chairman, Adani Group",
        "net_worth_estimate": 84_000_000_000,
        "influence_score": 0.88,
        "connections": [
            {"actor": "swf_gic", "type": "investment_partner"},
            {"actor": "swf_qatar_qia", "type": "investment_partner"},
        ],
        "political_affiliations": [
            {"party": "BJP_aligned", "note": "Close to PM Modi; Hindenburg short report in 2023 wiped $150B; recovered; infrastructure empire"},
        ],
        "known_positions": [
            {"sector": "ports", "type": "controlling_stake", "note": "Adani Ports: India's largest private port operator + Haifa port (Israel)"},
            {"sector": "energy", "type": "controlling_stake", "note": "Adani Green Energy, Adani Power, coal mining"},
            {"sector": "infrastructure", "type": "controlling_stake", "note": "Airports, roads, cement (Ambuja, ACC)"},
            {"sector": "media", "type": "controlling_stake", "note": "NDTV acquisition 2023"},
        ],
        "data_sources": ["bse_filings", "nse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "empire_builder",
    },
    "in_ratan_tata_legacy": {
        "name": "Ratan Tata (Legacy / Tata Trusts)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Tata Group — India's oldest and most diversified conglomerate",
        "net_worth_estimate": 1_000_000_000,  # personal; Tata Trusts control $150B group
        "aum": 150_000_000_000,  # Tata Group market cap
        "influence_score": 0.85,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Ratan Tata passed Oct 2024; Tata Trusts (66% of Tata Sons) now led by Noel Tata; Tata = Jaguar Land Rover, TCS, Tata Steel, Air India"},
        ],
        "known_positions": [
            {"sector": "technology", "type": "trust_controlled", "note": "TCS: India's largest IT company, $150B+ market cap"},
            {"sector": "steel", "type": "trust_controlled", "note": "Tata Steel: UK + India operations"},
            {"sector": "automotive", "type": "trust_controlled", "note": "Jaguar Land Rover, Tata Motors"},
            {"sector": "airlines", "type": "trust_controlled", "note": "Air India (re-acquired 2022)"},
        ],
        "data_sources": ["bse_filings", "nse_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "in_narayana_murthy": {
        "name": "Narayana Murthy",
        "tier": "individual",
        "category": "corporation",
        "title": "Co-Founder, Infosys",
        "net_worth_estimate": 4_800_000_000,
        "influence_score": 0.65,
        "connections": [
            {"actor": "uk_rishi_sunak", "type": "family"},
        ],
        "political_affiliations": [
            {"party": "independent", "note": "Father-in-law of Rishi Sunak; Infosys co-founder; India IT godfather"},
        ],
        "known_positions": [
            {"sector": "technology", "type": "founder_stake", "note": "Infosys: $75B+ IT services giant"},
        ],
        "data_sources": ["bse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "institutional_legacy",
    },
    "in_azim_premji": {
        "name": "Azim Premji",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder Chairman, Wipro; Azim Premji Foundation",
        "net_worth_estimate": 11_000_000_000,
        "influence_score": 0.62,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "India's biggest philanthropist; donated $21B+; Wipro founder"},
        ],
        "known_positions": [
            {"sector": "technology", "type": "founder_stake", "note": "Wipro: major IT services company"},
            {"sector": "philanthropy", "type": "foundation", "note": "Azim Premji Foundation: India's largest private philanthropy"},
        ],
        "data_sources": ["bse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "philanthropic",
    },
    "in_kumar_birla": {
        "name": "Kumar Mangalam Birla",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, Aditya Birla Group",
        "net_worth_estimate": 19_000_000_000,
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Birla dynasty; conglomerate across cement, metals, telecom, finance"},
        ],
        "known_positions": [
            {"sector": "cement", "type": "controlling_stake", "note": "UltraTech Cement: India's largest"},
            {"sector": "metals", "type": "controlling_stake", "note": "Hindalco (Novelis aluminium)"},
            {"sector": "telecom", "type": "minority_stake", "note": "Vodafone Idea (struggling)"},
            {"sector": "finance", "type": "controlling_stake", "note": "Aditya Birla Capital"},
        ],
        "data_sources": ["bse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "in_anand_mahindra": {
        "name": "Anand Mahindra",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, Mahindra Group",
        "net_worth_estimate": 3_500_000_000,
        "influence_score": 0.65,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Mahindra Group: automotive, IT (Tech Mahindra), agriculture, defense"},
        ],
        "known_positions": [
            {"sector": "automotive", "type": "controlling_family", "note": "Mahindra & Mahindra: SUVs, tractors, EVs"},
            {"sector": "technology", "type": "controlling_family", "note": "Tech Mahindra: IT services"},
            {"sector": "defense", "type": "controlling_family", "note": "Mahindra Defence Systems"},
        ],
        "data_sources": ["bse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "empire_builder",
    },
    "in_pm_modi": {
        "name": "Narendra Modi",
        "tier": "sovereign",
        "category": "government",
        "title": "Prime Minister of India",
        "influence_score": 0.94,
        "connections": [
            {"actor": "in_mukesh_ambani", "type": "political_alignment"},
            {"actor": "in_gautam_adani", "type": "political_alignment"},
            {"actor": "rbi_das", "type": "appoints"},
        ],
        "political_affiliations": [
            {"party": "BJP", "note": "Three-term PM; Make in India, Digital India; controls 1.4B population economy"},
        ],
        "data_sources": ["government_releases", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "political",
    },

    # ── China ──────────────────────────────────────────────────────────
    "cn_jack_ma": {
        "name": "Jack Ma (Ma Yun)",
        "tier": "individual",
        "category": "corporation",
        "title": "Co-Founder, Alibaba Group & Ant Group",
        "net_worth_estimate": 25_000_000_000,
        "influence_score": 0.65,  # reduced post-crackdown
        "connections": [
            {"actor": "jp_masayoshi_son", "type": "investment_partner"},
            {"actor": "cn_ccp_standing", "type": "regulated_by"},
        ],
        "political_affiliations": [
            {"party": "CCP_managed", "note": "Disappeared 2020 after criticizing regulators; Ant Group IPO killed; re-emerged 2023 diminished; cautionary tale of CCP vs private capital"},
        ],
        "known_positions": [
            {"sector": "ecommerce", "type": "founder_stake", "note": "Alibaba: China's largest e-commerce platform"},
            {"sector": "fintech", "type": "founder_stake", "note": "Ant Group: Alipay, world's largest fintech (pre-crackdown)"},
        ],
        "data_sources": ["hkex_filings", "sec_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "survival",
    },
    "cn_pony_ma": {
        "name": "Pony Ma (Ma Huateng)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Founder & CEO, Tencent Holdings",
        "net_worth_estimate": 35_000_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "cn_ccp_standing", "type": "regulated_by"},
        ],
        "political_affiliations": [
            {"party": "NPC_delegate", "note": "National People's Congress delegate; Tencent = WeChat (1.3B users), gaming, cloud, fintech"},
        ],
        "known_positions": [
            {"sector": "social_media", "type": "controlling_stake", "note": "WeChat/Weixin: 1.3B monthly users, China's everything-app"},
            {"sector": "gaming", "type": "controlling_stake", "note": "World's largest gaming company (Riot Games, Epic stake, Supercell)"},
            {"sector": "fintech", "type": "controlling_stake", "note": "WeChat Pay, Tencent Financial"},
            {"sector": "cloud", "type": "controlling_stake", "note": "Tencent Cloud: #2 in China"},
        ],
        "data_sources": ["hkex_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "cn_zhang_yiming": {
        "name": "Zhang Yiming",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder, ByteDance (TikTok/Douyin)",
        "net_worth_estimate": 43_000_000_000,
        "influence_score": 0.80,
        "connections": [
            {"actor": "cn_ccp_standing", "type": "regulated_by"},
        ],
        "political_affiliations": [
            {"party": "CCP_managed", "note": "Stepped down as CEO 2021 under political pressure; ByteDance valuation ~$220B; TikTok US ban saga ongoing"},
        ],
        "known_positions": [
            {"sector": "social_media", "type": "founder_stake", "note": "TikTok/Douyin: 1.5B+ monthly users globally"},
            {"sector": "AI", "type": "founder_stake", "note": "ByteDance AI: recommendation algorithms, LLMs"},
        ],
        "data_sources": ["public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "tech_visionary",
    },
    "cn_lei_jun": {
        "name": "Lei Jun",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder & CEO, Xiaomi Corporation",
        "net_worth_estimate": 17_000_000_000,
        "influence_score": 0.68,
        "connections": [
            {"actor": "cn_ccp_standing", "type": "regulated_by"},
        ],
        "political_affiliations": [
            {"party": "NPC_delegate", "note": "National People's Congress delegate; Xiaomi = phones, IoT, EVs"},
        ],
        "known_positions": [
            {"sector": "consumer_electronics", "type": "controlling_stake", "note": "Xiaomi: #3 global smartphone maker"},
            {"sector": "EV", "type": "new_entrant", "note": "Xiaomi SU7: surprise EV hit 2024-2025"},
            {"sector": "IoT", "type": "controlling_stake", "note": "World's largest consumer IoT platform"},
        ],
        "data_sources": ["hkex_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "market_share",
    },
    "cn_ren_zhengfei": {
        "name": "Ren Zhengfei",
        "tier": "institutional",
        "category": "corporation",
        "title": "Founder & CEO, Huawei Technologies",
        "net_worth_estimate": 2_000_000_000,
        "influence_score": 0.85,
        "connections": [
            {"actor": "cn_ccp_standing", "type": "strategic_asset"},
        ],
        "political_affiliations": [
            {"party": "CCP_aligned", "note": "Former PLA engineer; Huawei employee-owned; US sanctions since 2019; built parallel chip ecosystem"},
        ],
        "known_positions": [
            {"sector": "telecom_equipment", "type": "controlling_stake", "note": "Huawei: world's largest telecom equipment maker; 5G leader"},
            {"sector": "semiconductors", "type": "strategic", "note": "HiSilicon, Kirin chips — US sanctions forced domestic supply chain"},
            {"sector": "consumer_electronics", "type": "controlling_stake", "note": "Huawei phones, Mate 60 Pro (breakthrough chip)"},
            {"sector": "cloud", "type": "controlling_stake", "note": "Huawei Cloud: major China cloud provider"},
        ],
        "data_sources": ["public_reports", "sanctions_lists"],
        "credibility": "public_record",
        "motivation_model": "national_champion",
    },
    "cn_ccp_standing": {
        "name": "CCP Politburo Standing Committee",
        "tier": "sovereign",
        "category": "government",
        "title": "China's Supreme Decision-Making Body (7 members)",
        "influence_score": 0.99,
        "connections": [
            {"actor": "cn_xi_jinping", "type": "led_by"},
            {"actor": "pboc_pan", "type": "controls"},
            {"actor": "swf_cic", "type": "controls"},
            {"actor": "cn_safe", "type": "controls"},
        ],
        "political_affiliations": [
            {"party": "CCP", "note": "Xi Jinping (General Secretary), Li Qiang, Zhao Leji, Wang Huning, Cai Qi, Ding Xuexiang, Li Xi"},
        ],
        "known_positions": [
            {"sector": "all", "type": "sovereign_control", "note": "Controls $18T economy; SOEs; capital controls; can override any private enterprise"},
        ],
        "data_sources": ["government_releases", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "party_supremacy",
    },
    "cn_xi_jinping": {
        "name": "Xi Jinping",
        "tier": "sovereign",
        "category": "government",
        "title": "General Secretary, CCP; President, PRC; CMC Chairman",
        "influence_score": 0.99,
        "connections": [
            {"actor": "cn_ccp_standing", "type": "controls"},
            {"actor": "pboc_pan", "type": "appoints"},
            {"actor": "ru_putin", "type": "alliance"},
        ],
        "political_affiliations": [
            {"party": "CCP", "note": "Most powerful Chinese leader since Mao; abolished term limits; anti-corruption purges; common prosperity; tech regulation"},
        ],
        "data_sources": ["government_releases", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "party_supremacy",
    },
    "cn_state_council": {
        "name": "China State Council",
        "tier": "sovereign",
        "category": "government",
        "title": "Chief Administrative Authority of PRC (Li Qiang, Premier)",
        "influence_score": 0.95,
        "connections": [
            {"actor": "cn_ccp_standing", "type": "controlled_by"},
            {"actor": "cn_sasac", "type": "controls"},
        ],
        "data_sources": ["government_releases"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "cn_sasac": {
        "name": "SASAC (State-owned Assets Supervision & Administration Commission)",
        "tier": "sovereign",
        "category": "government",
        "title": "Controls 97 central SOEs with $9T+ combined assets",
        "aum": 9_000_000_000_000,
        "influence_score": 0.90,
        "connections": [
            {"actor": "cn_state_council", "type": "controlled_by"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "sovereign_control", "note": "PetroChina, Sinopec, CNOOC"},
            {"sector": "telecom", "type": "sovereign_control", "note": "China Mobile, China Telecom, China Unicom"},
            {"sector": "banking", "type": "sovereign_control", "note": "Big 4 banks (via Central Huijin)"},
            {"sector": "infrastructure", "type": "sovereign_control", "note": "China State Construction, China Railway"},
        ],
        "data_sources": ["government_releases", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "party_supremacy",
    },
    "cn_safe": {
        "name": "SAFE (State Administration of Foreign Exchange)",
        "tier": "sovereign",
        "category": "swf",
        "title": "Manages China's $3.2T foreign exchange reserves",
        "aum": 3_200_000_000_000,
        "influence_score": 0.92,
        "connections": [
            {"actor": "pboc_pan", "type": "controlled_by"},
            {"actor": "cn_ccp_standing", "type": "controlled_by"},
        ],
        "known_positions": [
            {"sector": "forex_reserves", "type": "sovereign_control", "note": "World's largest FX reserves; US Treasury holdings; gold accumulation"},
        ],
        "data_sources": ["safe_data", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "cn_petrochina": {
        "name": "PetroChina / CNPC",
        "tier": "institutional",
        "category": "corporation",
        "title": "China National Petroleum Corporation — Largest SOE",
        "aum": 350_000_000_000,
        "influence_score": 0.78,
        "connections": [
            {"actor": "cn_sasac", "type": "controlled_by"},
        ],
        "data_sources": ["hkex_filings", "sse_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "state_directed",
    },
    "cn_sinopec": {
        "name": "Sinopec Group",
        "tier": "institutional",
        "category": "corporation",
        "title": "China Petroleum & Chemical Corp — #2 oil SOE",
        "aum": 280_000_000_000,
        "influence_score": 0.75,
        "connections": [
            {"actor": "cn_sasac", "type": "controlled_by"},
        ],
        "data_sources": ["hkex_filings", "sse_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "state_directed",
    },

    # ── Southeast Asia ─────────────────────────────────────────────────
    "sea_robert_kuok": {
        "name": "Robert Kuok",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder, Kuok Group — Malaysia's Sugar King",
        "net_worth_estimate": 11_000_000_000,
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Malaysia's richest; commodities (palm oil, sugar), Shangri-La Hotels, SCMP (media)"},
        ],
        "known_positions": [
            {"sector": "commodities", "type": "controlling_stake", "note": "Wilmar International: world's largest palm oil trader"},
            {"sector": "hospitality", "type": "controlling_stake", "note": "Shangri-La Hotels"},
            {"sector": "media", "type": "ownership", "note": "South China Morning Post (sold to Alibaba 2016)"},
        ],
        "data_sources": ["sgx_filings", "public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },
    "sea_dhanin_cp": {
        "name": "Dhanin Chearavanont",
        "tier": "institutional",
        "category": "corporation",
        "title": "Senior Chairman, Charoen Pokphand (CP) Group, Thailand",
        "net_worth_estimate": 18_000_000_000,
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Thai-Chinese business dynasty; CP Group = agriculture, telecom (True), retail (7-Eleven in Thailand), finance"},
        ],
        "known_positions": [
            {"sector": "agriculture", "type": "controlling_stake", "note": "CP Foods: world's largest shrimp producer, major animal feed"},
            {"sector": "telecom", "type": "controlling_stake", "note": "True Corporation (merged with DTAC)"},
            {"sector": "retail", "type": "controlling_stake", "note": "CP ALL: 7-Eleven operator in Thailand (14,000+ stores)"},
        ],
        "data_sources": ["set_filings", "public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },
    "sea_mochtar_riady": {
        "name": "Mochtar Riady",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder, Lippo Group, Indonesia",
        "net_worth_estimate": 2_000_000_000,
        "influence_score": 0.58,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Indonesian tycoon; Lippo Group = real estate, hospitals, education, media; political donor controversy (US Clinton era)"},
        ],
        "known_positions": [
            {"sector": "real_estate", "type": "controlling_stake", "note": "Lippo Karawaci: major Indonesian property developer"},
            {"sector": "healthcare", "type": "controlling_stake", "note": "Siloam Hospitals: Indonesia's largest private hospital chain"},
        ],
        "data_sources": ["idx_filings", "public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },
    "sea_anthoni_salim": {
        "name": "Anthoni Salim",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, Salim Group, Indonesia",
        "net_worth_estimate": 8_000_000_000,
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Indonesia's richest family; Salim Group = Indofood (#1 instant noodle maker globally), banking, cement, telecom"},
        ],
        "known_positions": [
            {"sector": "food", "type": "controlling_stake", "note": "Indofood Sukses Makmur: world's largest instant noodle producer"},
            {"sector": "banking", "type": "controlling_stake", "note": "Bank Central Asia (BCA): Indonesia's most valuable bank"},
            {"sector": "telecom", "type": "controlling_stake", "note": "Indosat Ooredoo Hutchison"},
        ],
        "data_sources": ["idx_filings", "public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },
    "sea_temasek_ceo": {
        "name": "Dilhan Pillay Sandrasegara",
        "tier": "institutional",
        "category": "swf",
        "title": "CEO, Temasek Holdings (Singapore)",
        "influence_score": 0.72,
        "connections": [
            {"actor": "swf_temasek", "type": "same_entity"},
        ],
        "data_sources": ["sgx_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "sea_gic_ceo": {
        "name": "Lim Chow Kiat",
        "tier": "institutional",
        "category": "swf",
        "title": "CEO, GIC Private Limited (Singapore)",
        "influence_score": 0.72,
        "connections": [
            {"actor": "swf_gic", "type": "same_entity"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },

    # ── Hong Kong ──────────────────────────────────────────────────────
    "hk_li_ka_shing": {
        "name": "Li Ka-shing",
        "tier": "institutional",
        "category": "corporation",
        "title": "Founder, CK Hutchison Holdings",
        "net_worth_estimate": 35_000_000_000,
        "influence_score": 0.85,
        "connections": [
            {"actor": "hk_victor_li", "type": "family"},
        ],
        "political_affiliations": [
            {"party": "independent", "note": "Hong Kong's richest; 'Superman Li'; CK Hutchison = ports, telecom, energy, retail, infrastructure across 50+ countries"},
        ],
        "known_positions": [
            {"sector": "ports", "type": "controlling_stake", "note": "Hutchison Ports: world's largest port operator"},
            {"sector": "telecom", "type": "controlling_stake", "note": "CK Hutchison: Three (UK, Italy, Scandinavia), other carriers"},
            {"sector": "energy", "type": "controlling_stake", "note": "Husky Energy (Canada), Power Assets (HK)"},
            {"sector": "retail", "type": "controlling_stake", "note": "A.S. Watson: world's largest health & beauty retailer"},
            {"sector": "real_estate", "type": "controlling_stake", "note": "CK Asset Holdings: massive HK + global property"},
        ],
        "data_sources": ["hkex_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "hk_victor_li": {
        "name": "Victor Li Tzar Kuoi",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, CK Hutchison Holdings & CK Asset Holdings",
        "net_worth_estimate": 4_000_000_000,
        "influence_score": 0.72,
        "connections": [
            {"actor": "hk_li_ka_shing", "type": "family"},
        ],
        "data_sources": ["hkex_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "hk_lee_shau_kee": {
        "name": "Lee Shau Kee",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder, Henderson Land Development",
        "net_worth_estimate": 27_000_000_000,
        "influence_score": 0.70,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Hong Kong property tycoon; Henderson Land, Miramar Hotel, HK Gas"},
        ],
        "known_positions": [
            {"sector": "real_estate", "type": "controlling_stake", "note": "Henderson Land: major HK property developer"},
        ],
        "data_sources": ["hkex_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "hk_kwok_family": {
        "name": "Kwok Family (Raymond & Thomas Kwok)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Controlling Family, Sun Hung Kai Properties",
        "net_worth_estimate": 33_000_000_000,
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Sun Hung Kai Properties: HK's largest property developer; Thomas convicted of bribery (2014)"},
        ],
        "known_positions": [
            {"sector": "real_estate", "type": "controlling_family", "note": "Sun Hung Kai Properties: largest HK property developer"},
            {"sector": "telecom", "type": "controlling_stake", "note": "SmarTone Telecommunications"},
        ],
        "data_sources": ["hkex_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },

    # ══════════════════════════════════════════════════════════════════════
    # GLOBAL EXPANSION — EUROPE
    # ══════════════════════════════════════════════════════════════════════

    # ── United Kingdom ─────────────────────────────────────────────────
    "uk_rishi_sunak": {
        "name": "Rishi Sunak",
        "tier": "regional",
        "category": "politician",
        "title": "Former PM, United Kingdom (2022-2024)",
        "net_worth_estimate": 730_000_000,
        "influence_score": 0.72,
        "connections": [
            {"actor": "in_narayana_murthy", "type": "family"},
            {"actor": "uk_city_london", "type": "political_alignment"},
        ],
        "political_affiliations": [
            {"party": "Conservative", "note": "Former PM; Goldman Sachs + hedge fund background; father-in-law = Infosys co-founder; significant Indian business connections"},
        ],
        "data_sources": ["uk_parliament_disclosures", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "political",
    },
    "uk_city_london": {
        "name": "City of London Financial District",
        "tier": "institutional",
        "category": "corporation",
        "title": "City of London — Global Financial Hub (HSBC, Barclays, Lloyds, Standard Chartered)",
        "aum": 10_000_000_000_000,  # approximate assets managed through City
        "influence_score": 0.90,
        "connections": [
            {"actor": "boe_bailey", "type": "regulated_by"},
        ],
        "known_positions": [
            {"sector": "banking", "type": "hub", "note": "HSBC ($3T assets), Barclays, Lloyds, Standard Chartered, NatWest"},
            {"sector": "insurance", "type": "hub", "note": "Lloyd's of London, Aviva, Legal & General"},
            {"sector": "forex", "type": "hub", "note": "37% of global FX trading volume"},
        ],
        "data_sources": ["fca_filings", "lse_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "uk_james_dyson": {
        "name": "James Dyson",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder & Chief Engineer, Dyson",
        "net_worth_estimate": 23_000_000_000,
        "influence_score": 0.55,
        "connections": [],
        "political_affiliations": [
            {"party": "Conservative", "note": "Pro-Brexit; Singapore-based; UK's richest person by some measures"},
        ],
        "known_positions": [
            {"sector": "consumer_electronics", "type": "controlling_stake", "note": "Dyson: vacuum, air treatment, hair care; moved HQ to Singapore"},
            {"sector": "agriculture", "type": "direct_ownership", "note": "UK's largest individual farmer (~36,000 acres)"},
        ],
        "data_sources": ["public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "profit_maximizer",
    },
    "uk_hinduja_brothers": {
        "name": "Hinduja Family (Gopichand, Srichand legacy, Prakash, Ashok)",
        "tier": "individual",
        "category": "corporation",
        "title": "Hinduja Group — UK's Richest Family",
        "net_worth_estimate": 35_000_000_000,
        "influence_score": 0.65,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "UK's wealthiest family; Hinduja Group = banking, oil, automotive, healthcare, media"},
        ],
        "known_positions": [
            {"sector": "banking", "type": "controlling_stake", "note": "IndusInd Bank (India)"},
            {"sector": "oil_trading", "type": "controlling_stake", "note": "Gulf Oil, Hinduja Group trading"},
            {"sector": "automotive", "type": "controlling_stake", "note": "Ashok Leyland: India's #2 truck maker"},
            {"sector": "media", "type": "controlling_stake", "note": "TV channel interests"},
        ],
        "data_sources": ["public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },
    "uk_duke_westminster": {
        "name": "Hugh Grosvenor, Duke of Westminster",
        "tier": "individual",
        "category": "corporation",
        "title": "Head, Grosvenor Group — UK's Largest Private Landowner",
        "net_worth_estimate": 13_000_000_000,
        "influence_score": 0.62,
        "connections": [],
        "political_affiliations": [
            {"party": "Conservative", "note": "Grosvenor Group: $80B+ global real estate (Mayfair, Belgravia, international); godfather to Prince George"},
        ],
        "known_positions": [
            {"sector": "real_estate", "type": "controlling_family", "note": "Grosvenor Group: 300 acres of Mayfair & Belgravia + global portfolio"},
        ],
        "data_sources": ["companies_house", "public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },

    # ── France ─────────────────────────────────────────────────────────
    "fr_bernard_arnault": {
        "name": "Bernard Arnault",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman & CEO, LVMH Moet Hennessy Louis Vuitton",
        "net_worth_estimate": 185_000_000_000,
        "influence_score": 0.92,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "World's richest person (2023-2024); LVMH = Louis Vuitton, Dior, Tiffany, Hennessy, Sephora; 75 luxury brands; France's most powerful businessman"},
        ],
        "known_positions": [
            {"sector": "luxury", "type": "controlling_stake", "note": "LVMH: world's largest luxury conglomerate ($400B+ market cap)"},
            {"sector": "retail", "type": "controlling_stake", "note": "Sephora, DFS, Le Bon Marche"},
            {"sector": "media", "type": "controlling_stake", "note": "Les Echos, Le Parisien (French media)"},
            {"sector": "hospitality", "type": "controlling_stake", "note": "Belmond hotels, Cheval Blanc"},
        ],
        "data_sources": ["euronext_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "empire_builder",
    },
    "fr_francois_pinault": {
        "name": "Francois-Henri Pinault",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman & CEO, Kering (Gucci, YSL, Balenciaga)",
        "net_worth_estimate": 22_000_000_000,
        "influence_score": 0.72,
        "connections": [
            {"actor": "fr_bernard_arnault", "type": "peer"},
        ],
        "political_affiliations": [
            {"party": "independent", "note": "Pinault family: Kering luxury, Christie's auction house, art collection; married to Salma Hayek"},
        ],
        "known_positions": [
            {"sector": "luxury", "type": "controlling_stake", "note": "Kering: Gucci, YSL, Balenciaga, Bottega Veneta, Alexander McQueen"},
            {"sector": "art_auction", "type": "controlling_stake", "note": "Christie's: world's leading auction house"},
        ],
        "data_sources": ["euronext_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "fr_dassault_family": {
        "name": "Dassault Family (Charles Edelstenne, steward)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Dassault Group — French Defense, Aviation & Media Dynasty",
        "net_worth_estimate": 28_000_000_000,
        "influence_score": 0.78,
        "connections": [],
        "political_affiliations": [
            {"party": "center_right", "note": "Dassault Group: Rafale fighter jets, Falcon business jets, Le Figaro newspaper, Dassault Systemes ($60B+ software)"},
        ],
        "known_positions": [
            {"sector": "defense", "type": "controlling_family", "note": "Dassault Aviation: Rafale fighter jets, global defense contracts"},
            {"sector": "software", "type": "controlling_family", "note": "Dassault Systemes: $60B+ 3D design/simulation software"},
            {"sector": "media", "type": "controlling_family", "note": "Le Figaro: major French newspaper"},
        ],
        "data_sources": ["euronext_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "fr_total_pouyanne": {
        "name": "Patrick Pouyanne",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, TotalEnergies",
        "influence_score": 0.72,
        "connections": [],
        "known_positions": [
            {"sector": "energy", "type": "corporate_leadership", "note": "TotalEnergies: Europe's #2 oil major; major LNG trader; Africa operations"},
        ],
        "data_sources": ["euronext_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "fr_engie_clamadieu": {
        "name": "Jean-Pierre Clamadieu",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, Engie (French energy utility)",
        "influence_score": 0.60,
        "connections": [],
        "known_positions": [
            {"sector": "energy", "type": "corporate_leadership", "note": "Engie: major European utility; natural gas, renewables"},
        ],
        "data_sources": ["euronext_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },

    # ── Germany ────────────────────────────────────────────────────────
    "de_quandt_family": {
        "name": "Quandt/Klatten Family (Stefan Quandt, Susanne Klatten)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Controlling Family, BMW Group",
        "net_worth_estimate": 45_000_000_000,
        "influence_score": 0.78,
        "connections": [],
        "political_affiliations": [
            {"party": "CDU_donors", "note": "Germany's richest family; BMW controlling shareholders (47%); Altana, SGL Carbon"},
        ],
        "known_positions": [
            {"sector": "automotive", "type": "controlling_family", "note": "BMW Group: BMW, Mini, Rolls-Royce Motors; $65B+ revenue"},
            {"sector": "chemicals", "type": "controlling_stake", "note": "Altana AG specialty chemicals"},
        ],
        "data_sources": ["xetra_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "de_schwarz_dieter": {
        "name": "Dieter Schwarz",
        "tier": "individual",
        "category": "corporation",
        "title": "Owner, Schwarz Group (Lidl, Kaufland)",
        "net_worth_estimate": 47_000_000_000,
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Germany's richest person; Schwarz Group: Lidl (#4 global retailer), Kaufland; extremely private"},
        ],
        "known_positions": [
            {"sector": "retail", "type": "controlling_stake", "note": "Lidl: 12,000+ stores in 30+ countries; Kaufland hypermarkets"},
        ],
        "data_sources": ["public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "profit_maximizer",
    },
    "de_siemens_busch": {
        "name": "Roland Busch",
        "tier": "institutional",
        "category": "corporation",
        "title": "President & CEO, Siemens AG",
        "influence_score": 0.68,
        "connections": [],
        "known_positions": [
            {"sector": "industrial", "type": "corporate_leadership", "note": "Siemens: $80B+ revenue; automation, digitalization, smart infrastructure"},
            {"sector": "healthcare", "type": "spinoff", "note": "Siemens Healthineers (separately listed)"},
        ],
        "data_sources": ["xetra_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "de_deutsche_bank_sewing": {
        "name": "Christian Sewing",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, Deutsche Bank",
        "influence_score": 0.72,
        "connections": [
            {"actor": "uk_city_london", "type": "peer"},
        ],
        "known_positions": [
            {"sector": "banking", "type": "corporate_leadership", "note": "Deutsche Bank: Germany's largest bank; major derivatives dealer; Epstein connections (fined); Trump loans"},
        ],
        "data_sources": ["xetra_filings", "sec_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "de_allianz_baete": {
        "name": "Oliver Baete",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, Allianz SE",
        "influence_score": 0.68,
        "connections": [],
        "known_positions": [
            {"sector": "insurance", "type": "corporate_leadership", "note": "Allianz: world's largest insurance company; PIMCO parent"},
            {"sector": "asset_management", "type": "parent_company", "note": "Allianz Global Investors + PIMCO = $2.4T+ AUM"},
        ],
        "data_sources": ["xetra_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "de_bosch_denner_legacy": {
        "name": "Bosch Group (Robert Bosch Stiftung)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Bosch Group — Foundation-owned industrial giant",
        "aum": 95_000_000_000,  # revenue
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "92% owned by Robert Bosch Stiftung (charity); no family control; world's largest auto parts supplier"},
        ],
        "known_positions": [
            {"sector": "automotive_parts", "type": "foundation_owned", "note": "World's largest auto parts supplier; IoT, industrial tech"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },

    # ── Switzerland ────────────────────────────────────────────────────
    "ch_glencore_nagle": {
        "name": "Gary Nagle",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, Glencore (mining + commodity trading)",
        "influence_score": 0.78,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Glencore: world's largest commodity trading company; bribery convictions ($1.1B fine 2022); mines + trades oil, metals, coal"},
        ],
        "known_positions": [
            {"sector": "mining", "type": "corporate_leadership", "note": "Glencore: cobalt, copper, zinc, nickel, coal mining"},
            {"sector": "commodity_trading", "type": "corporate_leadership", "note": "World's largest commodity trader; oil, metals, agriculture"},
        ],
        "data_sources": ["lse_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "profit_maximizer",
    },
    "ch_nestle_schneider": {
        "name": "Laurent Freixe",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, Nestle (world's largest food company)",
        "influence_score": 0.72,
        "connections": [],
        "known_positions": [
            {"sector": "food", "type": "corporate_leadership", "note": "Nestle: $95B+ revenue; Nespresso, KitKat, Purina, Gerber; world's largest food company"},
        ],
        "data_sources": ["six_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ch_novartis_narasimhan": {
        "name": "Vas Narasimhan",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, Novartis AG",
        "influence_score": 0.68,
        "connections": [],
        "known_positions": [
            {"sector": "pharma", "type": "corporate_leadership", "note": "Novartis: $50B+ revenue; major innovative pharma; spun off Sandoz (generics)"},
        ],
        "data_sources": ["six_filings", "sec_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "ch_ubs_ermotti": {
        "name": "Sergio Ermotti",
        "tier": "institutional",
        "category": "corporation",
        "title": "CEO, UBS Group (post-Credit Suisse merger)",
        "influence_score": 0.82,
        "connections": [
            {"actor": "uk_city_london", "type": "peer"},
        ],
        "known_positions": [
            {"sector": "banking", "type": "corporate_leadership", "note": "UBS: $5.7T total assets (post-CS merger); world's largest wealth manager; Swiss national champion"},
            {"sector": "wealth_management", "type": "dominant", "note": "UBS Global Wealth Management: $4T+ AUM"},
        ],
        "data_sources": ["six_filings", "sec_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ── Italy ──────────────────────────────────────────────────────────
    "it_elkann_john": {
        "name": "John Elkann",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, Stellantis & CEO, Exor (Agnelli family holding)",
        "net_worth_estimate": 2_200_000_000,
        "influence_score": 0.78,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Agnelli dynasty heir; Exor = Stellantis, Ferrari, CNH Industrial, Juventus FC, The Economist, GEDI media group"},
        ],
        "known_positions": [
            {"sector": "automotive", "type": "controlling_family", "note": "Stellantis (Fiat, Chrysler, Peugeot, Maserati), Ferrari"},
            {"sector": "media", "type": "controlling_family", "note": "The Economist Group, GEDI (La Repubblica, La Stampa)"},
            {"sector": "agriculture_machinery", "type": "controlling_family", "note": "CNH Industrial (Case, New Holland)"},
            {"sector": "sports", "type": "controlling_family", "note": "Juventus FC"},
            {"sector": "reinsurance", "type": "controlling_family", "note": "PartnerRe"},
        ],
        "data_sources": ["euronext_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "it_berlusconi_legacy": {
        "name": "Berlusconi Family (Marina & Pier Silvio Berlusconi)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Fininvest — Berlusconi media & finance empire",
        "net_worth_estimate": 7_000_000_000,
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "Forza_Italia", "note": "Silvio Berlusconi died June 2023; children control Fininvest = Mediaset (TV), Mondadori (publishing), Banca Mediolanum, AC Monza"},
        ],
        "known_positions": [
            {"sector": "media", "type": "controlling_family", "note": "Mediaset (MFE): Italy's dominant private broadcaster"},
            {"sector": "banking", "type": "controlling_family", "note": "Banca Mediolanum"},
            {"sector": "publishing", "type": "controlling_family", "note": "Mondadori"},
        ],
        "data_sources": ["borsa_italiana_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "it_del_vecchio_legacy": {
        "name": "Del Vecchio Family (EssilorLuxottica)",
        "tier": "institutional",
        "category": "corporation",
        "title": "EssilorLuxottica — World's dominant eyewear company",
        "net_worth_estimate": 40_000_000_000,  # family total via Delfin
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Leonardo Del Vecchio died June 2022; six children + Delfin holding; EssilorLuxottica = Ray-Ban, Oakley, LensCrafters; also major Mediobanca + Generali stakes"},
        ],
        "known_positions": [
            {"sector": "eyewear", "type": "controlling_family", "note": "EssilorLuxottica: monopoly (Ray-Ban, Oakley, LensCrafters, Sunglass Hut); $90B+ market cap"},
            {"sector": "banking", "type": "major_stake", "note": "Mediobanca: Italy's premier investment bank"},
            {"sector": "insurance", "type": "major_stake", "note": "Generali: Italy's largest insurer"},
        ],
        "data_sources": ["euronext_filings", "borsa_italiana_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },

    # ── Russia (sanctioned but still influential) ──────────────────────
    "ru_putin": {
        "name": "Vladimir Putin",
        "tier": "sovereign",
        "category": "government",
        "title": "President, Russian Federation",
        "net_worth_estimate": 200_000_000_000,  # estimated hidden wealth
        "influence_score": 0.95,
        "connections": [
            {"actor": "cn_xi_jinping", "type": "alliance"},
            {"actor": "ru_gazprom", "type": "controls"},
            {"actor": "ru_rosneft", "type": "controls"},
        ],
        "political_affiliations": [
            {"party": "United_Russia", "note": "Authoritarian ruler; estimated $200B hidden wealth (Navalny investigations, Panama Papers); controls Russia's $1.8T economy through oligarch network"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "sovereign_control", "note": "Gazprom, Rosneft; energy as geopolitical weapon"},
            {"sector": "defense", "type": "sovereign_control", "note": "Rostec, Russia's defense industrial complex"},
            {"sector": "metals", "type": "through_oligarchs", "note": "Nornickel, Rusal via Potanin/Deripaska"},
        ],
        "data_sources": ["sanctions_lists", "icij_leaks", "public_reports"],
        "credibility": "inferred",
        "motivation_model": "power_preservation",
    },
    "ru_gazprom": {
        "name": "Gazprom",
        "tier": "institutional",
        "category": "corporation",
        "title": "Gazprom — Russia's state gas giant",
        "aum": 200_000_000_000,
        "influence_score": 0.78,
        "connections": [
            {"actor": "ru_putin", "type": "controlled_by"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "state_controlled", "note": "World's largest natural gas company; controls ~15% of global gas reserves; pipeline network to Europe & China"},
        ],
        "data_sources": ["sanctions_lists", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "state_directed",
    },
    "ru_rosneft": {
        "name": "Rosneft (Igor Sechin, CEO)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Rosneft — Russia's #1 oil company",
        "aum": 150_000_000_000,
        "influence_score": 0.75,
        "connections": [
            {"actor": "ru_putin", "type": "controlled_by"},
        ],
        "political_affiliations": [
            {"party": "Putin_inner_circle", "note": "Igor Sechin = 'Darth Vader' of Russian oil; Putin's most trusted operative; sanctioned"},
        ],
        "data_sources": ["sanctions_lists", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "state_directed",
    },
    "ru_oligarch_network": {
        "name": "Russian Oligarch Network",
        "tier": "institutional",
        "category": "corporation",
        "title": "Putin's Inner Circle Oligarchs (sanctioned network)",
        "net_worth_estimate": 500_000_000_000,  # combined estimated
        "influence_score": 0.72,
        "connections": [
            {"actor": "ru_putin", "type": "controlled_by"},
        ],
        "political_affiliations": [
            {"party": "Putin_network", "note": "Key oligarchs: Alisher Usmanov (metals/tech), Vladimir Potanin (Nornickel), Oleg Deripaska (Rusal), Mikhail Fridman (Alfa Group), Roman Abramovich (sanctioned), Gennady Timchenko (energy), Arkady Rotenberg (infrastructure)"},
        ],
        "known_positions": [
            {"sector": "metals", "type": "oligarch_controlled", "note": "Nornickel (Potanin), Rusal (Deripaska)"},
            {"sector": "banking", "type": "oligarch_controlled", "note": "Alfa Bank (Fridman)"},
            {"sector": "energy", "type": "oligarch_controlled", "note": "Novatek LNG (Timchenko)"},
        ],
        "data_sources": ["sanctions_lists", "icij_leaks", "public_reports", "ofac"],
        "credibility": "inferred",
        "motivation_model": "power_preservation",
    },

    # ── Nordic ─────────────────────────────────────────────────────────
    "nordic_kamprad_legacy": {
        "name": "Kamprad Family / Ingka Group (IKEA)",
        "tier": "institutional",
        "category": "corporation",
        "title": "IKEA / Ingka Group — World's largest furniture retailer",
        "net_worth_estimate": 60_000_000_000,  # family + foundation wealth
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Ingvar Kamprad died 2018; IKEA controlled through complex foundation structure (Stichting INGKA, Interogo); 460+ stores in 60+ countries"},
        ],
        "known_positions": [
            {"sector": "retail", "type": "foundation_controlled", "note": "IKEA: $47B+ revenue; world's largest furniture retailer"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "nordic_daniel_ek": {
        "name": "Daniel Ek",
        "tier": "individual",
        "category": "corporation",
        "title": "Co-Founder & CEO, Spotify",
        "net_worth_estimate": 7_000_000_000,
        "influence_score": 0.58,
        "connections": [],
        "known_positions": [
            {"sector": "media", "type": "controlling_stake", "note": "Spotify: 600M+ users, world's largest music streaming platform"},
        ],
        "data_sources": ["nyse_filings", "sec_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "tech_visionary",
    },
    "nordic_geely_volvo": {
        "name": "Li Shufu / Geely-Volvo Connection",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, Geely Auto — Controls Volvo Cars, Polestar, Lotus",
        "net_worth_estimate": 7_500_000_000,
        "influence_score": 0.68,
        "connections": [
            {"actor": "cn_ccp_standing", "type": "regulated_by"},
        ],
        "political_affiliations": [
            {"party": "CCP_aligned", "note": "Chinese billionaire who acquired Volvo (2010), largest Daimler/Mercedes shareholder (9.7%), Polestar, Lotus; bridges China-Europe auto industry"},
        ],
        "known_positions": [
            {"sector": "automotive", "type": "controlling_stake", "note": "Geely Auto, Volvo Cars, Polestar, Lotus, Lynk & Co"},
            {"sector": "automotive", "type": "major_stake", "note": "Mercedes-Benz: 9.7% stake (largest shareholder)"},
        ],
        "data_sources": ["hkex_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "empire_builder",
    },

    # ══════════════════════════════════════════════════════════════════════
    # GLOBAL EXPANSION — MIDDLE EAST (beyond existing)
    # ══════════════════════════════════════════════════════════════════════

    # ── Turkey ─────────────────────────────────────────────────────────
    "tr_erdogan": {
        "name": "Recep Tayyip Erdogan",
        "tier": "sovereign",
        "category": "government",
        "title": "President, Republic of Turkey",
        "influence_score": 0.88,
        "connections": [
            {"actor": "ru_putin", "type": "strategic_partner"},
            {"actor": "royal_mbs", "type": "strategic_partner"},
        ],
        "political_affiliations": [
            {"party": "AKP", "note": "Authoritarian-leaning leader; controls $900B+ economy; construction boom/bust; lira crisis; business cronies in construction/energy"},
        ],
        "known_positions": [
            {"sector": "all", "type": "sovereign_control", "note": "Turkey's $900B economy; CBRT interest rate manipulation; construction-driven growth"},
        ],
        "data_sources": ["government_releases", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "power_preservation",
    },
    "tr_koc_holding": {
        "name": "Koc Holding (Koc Family)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Koc Holding — Turkey's Largest Conglomerate",
        "net_worth_estimate": 6_000_000_000,
        "aum": 50_000_000_000,  # group revenue
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "secular_business", "note": "Turkey's largest industrial conglomerate; automotive (Ford JV), energy (Tupras refinery), banking (Yapi Kredi), appliances (Arcelik/Beko)"},
        ],
        "known_positions": [
            {"sector": "automotive", "type": "controlling_family", "note": "Ford Otosan JV (Turkey's #1 automaker), Tofas (Fiat JV)"},
            {"sector": "energy", "type": "controlling_family", "note": "Tupras: Turkey's only oil refinery"},
            {"sector": "banking", "type": "controlling_family", "note": "Yapi Kredi Bank"},
            {"sector": "appliances", "type": "controlling_family", "note": "Arcelik/Beko: global appliance brand"},
        ],
        "data_sources": ["bist_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "tr_sabanci": {
        "name": "Sabanci Holding (Sabanci Family)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Sabanci Holding — Turkey's #2 Conglomerate",
        "net_worth_estimate": 4_000_000_000,
        "influence_score": 0.65,
        "connections": [],
        "political_affiliations": [
            {"party": "secular_business", "note": "Sabanci Group: banking (Akbank), energy, cement, retail; historically secular business establishment"},
        ],
        "known_positions": [
            {"sector": "banking", "type": "controlling_family", "note": "Akbank: Turkey's major private bank"},
            {"sector": "energy", "type": "controlling_family", "note": "Enerjisa: Turkey's largest power distributor"},
            {"sector": "cement", "type": "controlling_family"},
        ],
        "data_sources": ["bist_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },

    # ── Iran ────────────────────────────────────────────────────────────
    "ir_irgc_economic": {
        "name": "IRGC (Islamic Revolutionary Guard Corps) Economic Empire",
        "tier": "sovereign",
        "category": "government",
        "title": "IRGC — Controls ~60% of Iran's Economy",
        "aum": 500_000_000_000,  # estimated economic control
        "influence_score": 0.88,
        "connections": [
            {"actor": "ir_supreme_leader", "type": "controlled_by"},
        ],
        "political_affiliations": [
            {"party": "hardline_conservative", "note": "IRGC controls construction (Khatam al-Anbiya), telecom, oil, banking, smuggling; estimated 60% of economy; sanctioned by US/EU"},
        ],
        "known_positions": [
            {"sector": "construction", "type": "military_controlled", "note": "Khatam al-Anbiya: IRGC construction arm, major infrastructure"},
            {"sector": "energy", "type": "military_controlled", "note": "Controls significant oil/gas operations through front companies"},
            {"sector": "telecom", "type": "military_controlled"},
            {"sector": "finance", "type": "military_controlled", "note": "Controls banks, bonyads (foundations)"},
        ],
        "data_sources": ["sanctions_lists", "ofac", "public_reports"],
        "credibility": "inferred",
        "motivation_model": "power_preservation",
    },
    "ir_supreme_leader": {
        "name": "Ali Khamenei (Supreme Leader of Iran)",
        "tier": "sovereign",
        "category": "government",
        "title": "Supreme Leader, Islamic Republic of Iran",
        "net_worth_estimate": 200_000_000_000,  # Setad conglomerate estimated
        "influence_score": 0.92,
        "connections": [
            {"actor": "ir_irgc_economic", "type": "controls"},
        ],
        "political_affiliations": [
            {"party": "theocratic_ruler", "note": "Controls Setad: $95B+ conglomerate (real estate, telecom, oil); executive/judicial/military oversight; sanctions evasion networks"},
        ],
        "data_sources": ["sanctions_lists", "reuters_investigations", "public_reports"],
        "credibility": "inferred",
        "motivation_model": "power_preservation",
    },

    # ── Israel ─────────────────────────────────────────────────────────
    "il_gil_shwed": {
        "name": "Gil Shwed",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder & Chairman, Check Point Software Technologies",
        "net_worth_estimate": 5_000_000_000,
        "influence_score": 0.58,
        "connections": [],
        "known_positions": [
            {"sector": "cybersecurity", "type": "founder_stake", "note": "Check Point: pioneered firewall technology; major global cybersecurity company"},
        ],
        "data_sources": ["sec_filings", "nasdaq_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "tech_visionary",
    },
    "il_teva": {
        "name": "Teva Pharmaceutical Industries",
        "tier": "institutional",
        "category": "corporation",
        "title": "Teva — World's Largest Generic Drug Maker",
        "influence_score": 0.65,
        "connections": [],
        "known_positions": [
            {"sector": "pharma", "type": "corporate_leadership", "note": "Teva: world's largest generic drug manufacturer; opioid litigation; $15B+ revenue"},
        ],
        "data_sources": ["sec_filings", "tase_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "il_elbit_systems": {
        "name": "Elbit Systems",
        "tier": "institutional",
        "category": "corporation",
        "title": "Elbit Systems — Israel's Largest Defense Company",
        "influence_score": 0.68,
        "connections": [],
        "known_positions": [
            {"sector": "defense", "type": "corporate_leadership", "note": "Elbit: drones, electronic warfare, C4ISR; Israeli defense backbone; $6B+ revenue"},
        ],
        "data_sources": ["sec_filings", "tase_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "il_rafael": {
        "name": "Rafael Advanced Defense Systems",
        "tier": "institutional",
        "category": "corporation",
        "title": "Rafael — Iron Dome, David's Sling Developer (state-owned)",
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "Israeli_government", "note": "State-owned; Iron Dome, Trophy APS, Spike missiles; critical US defense partner"},
        ],
        "known_positions": [
            {"sector": "defense", "type": "state_owned", "note": "Rafael: Iron Dome, David's Sling, Spike missiles; joint ventures with Raytheon"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "national_security",
    },
    "il_tech_ecosystem": {
        "name": "Israel Tech Ecosystem (Startup Nation)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Israel Tech Hub — 7000+ startups, $20B+ VC annually",
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "More NASDAQ-listed companies than any country except US/China; NSO Group (Pegasus spyware); Unit 8200 alumni pipeline; AI/cyber/biotech hub"},
        ],
        "known_positions": [
            {"sector": "cybersecurity", "type": "ecosystem", "note": "CyberArk, Wiz ($32B sale to Google), Palo Alto Networks founders"},
            {"sector": "AI", "type": "ecosystem", "note": "Mobileye (Intel), AI21 Labs, Hailo"},
            {"sector": "enterprise_tech", "type": "ecosystem", "note": "monday.com, Fiverr, Payoneer, ironSource"},
        ],
        "data_sources": ["sec_filings", "tase_filings", "public_reports"],
        "credibility": "public_record",
        "motivation_model": "innovation",
    },

    # ══════════════════════════════════════════════════════════════════════
    # GLOBAL EXPANSION — AFRICA
    # ══════════════════════════════════════════════════════════════════════
    "af_aliko_dangote": {
        "name": "Aliko Dangote",
        "tier": "institutional",
        "category": "corporation",
        "title": "Founder & Chairman, Dangote Group — Africa's Richest Person",
        "net_worth_estimate": 20_000_000_000,
        "influence_score": 0.82,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Africa's richest person; Dangote Refinery ($20B, Africa's largest, 650K bpd); Dangote Cement (Africa's largest); close to multiple Nigerian presidents"},
        ],
        "known_positions": [
            {"sector": "refining", "type": "controlling_stake", "note": "Dangote Refinery: Africa's largest, 650,000 bpd; will reshape African fuel imports"},
            {"sector": "cement", "type": "controlling_stake", "note": "Dangote Cement: Africa's #1 producer; operations in 10 African countries"},
            {"sector": "commodities", "type": "controlling_stake", "note": "Sugar, salt, flour, fertilizer across Africa"},
        ],
        "data_sources": ["nse_ng_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "empire_builder",
    },
    "af_johann_rupert": {
        "name": "Johann Rupert",
        "tier": "individual",
        "category": "corporation",
        "title": "Chairman, Compagnie Financiere Richemont (Cartier, Montblanc)",
        "net_worth_estimate": 12_000_000_000,
        "influence_score": 0.68,
        "connections": [
            {"actor": "fr_bernard_arnault", "type": "peer"},
        ],
        "political_affiliations": [
            {"party": "independent", "note": "South Africa's richest; Richemont = Cartier, Van Cleef & Arpels, IWC, Montblanc, Net-a-Porter; Swiss-listed"},
        ],
        "known_positions": [
            {"sector": "luxury", "type": "controlling_stake", "note": "Richemont: world's #2 luxury group (Cartier, Van Cleef); $80B+ market cap"},
            {"sector": "technology", "type": "controlling_stake", "note": "Remgro: diversified South African investment holding"},
        ],
        "data_sources": ["six_filings", "jse_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "dynastic_preservation",
    },
    "af_nicky_oppenheimer": {
        "name": "Nicky Oppenheimer",
        "tier": "individual",
        "category": "corporation",
        "title": "Former Chairman, De Beers — Diamond Industry Legacy",
        "net_worth_estimate": 8_000_000_000,
        "influence_score": 0.62,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Oppenheimer family controlled De Beers 1927-2012 (sold to Anglo American); now invests via Oppenheimer Generations/Fireblade Aviation; $8B+ family wealth"},
        ],
        "known_positions": [
            {"sector": "mining", "type": "former_dynasty", "note": "De Beers legacy; now Oppenheimer Generations investment vehicle"},
            {"sector": "conservation", "type": "philanthropy", "note": "Major African wildlife conservation"},
        ],
        "data_sources": ["public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "dynastic_preservation",
    },
    "af_strive_masiyiwa": {
        "name": "Strive Masiyiwa",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder & Chairman, Econet Wireless — Zimbabwe Telecoms Pioneer",
        "net_worth_estimate": 1_500_000_000,
        "influence_score": 0.55,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Zimbabwe-born; Econet Wireless: pan-African telecom; fought Mugabe regime for license; Netflix board member; philanthropy focus"},
        ],
        "known_positions": [
            {"sector": "telecom", "type": "controlling_stake", "note": "Econet Wireless: Zimbabwe, Burundi, Lesotho; Liquid Intelligent Technologies (pan-African fibre)"},
        ],
        "data_sources": ["public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "impact_investor",
    },
    "af_mo_ibrahim": {
        "name": "Mo Ibrahim",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder, Celtel International; Mo Ibrahim Foundation",
        "net_worth_estimate": 1_100_000_000,
        "influence_score": 0.55,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Sudanese-British; sold Celtel to Zain ($3.4B, 2005); Mo Ibrahim Foundation: Africa governance index; Ibrahim Prize for Achievement in African Leadership"},
        ],
        "known_positions": [
            {"sector": "telecom", "type": "former_founder", "note": "Celtel (sold); now focused on governance/philanthropy"},
            {"sector": "governance", "type": "foundation", "note": "Mo Ibrahim Index: tracks African governance quality"},
        ],
        "data_sources": ["public_reports", "forbes"],
        "credibility": "public_record",
        "motivation_model": "philanthropic",
    },
    "af_afdb_adesina": {
        "name": "Akinwumi Adesina",
        "tier": "regional",
        "category": "government",
        "title": "President, African Development Bank",
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "multilateral", "note": "AfDB: $250B+ lending portfolio; Africa's premier development finance institution; infrastructure, energy, agriculture financing"},
        ],
        "known_positions": [
            {"sector": "development_finance", "type": "institutional_leadership", "note": "AfDB: major infrastructure & energy financier across 54 African countries"},
        ],
        "data_sources": ["afdb_reports", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "af_swf_botswana_pula": {
        "name": "Pula Fund (Botswana)",
        "tier": "institutional",
        "category": "swf",
        "title": "Botswana Pula Fund — Diamond-Revenue SWF",
        "aum": 5_000_000_000,
        "influence_score": 0.42,
        "connections": [],
        "known_positions": [
            {"sector": "mining", "type": "revenue_source", "note": "Funded by Debswana diamond revenues; Botswana's fiscal stabilization fund"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "af_swf_nigeria_nsia": {
        "name": "NSIA (Nigeria Sovereign Investment Authority)",
        "tier": "institutional",
        "category": "swf",
        "title": "Nigeria Sovereign Investment Authority",
        "aum": 3_000_000_000,
        "influence_score": 0.42,
        "connections": [],
        "known_positions": [
            {"sector": "infrastructure", "type": "sovereign_investment", "note": "Three funds: stabilization, future generations, infrastructure"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "public_record",
        "motivation_model": "institutional_mandate",
    },
    "af_swf_angola": {
        "name": "FSDEA (Fundo Soberano de Angola)",
        "tier": "institutional",
        "category": "swf",
        "title": "Angola Sovereign Wealth Fund",
        "aum": 3_400_000_000,
        "influence_score": 0.38,
        "connections": [],
        "political_affiliations": [
            {"party": "MPLA", "note": "Oil-funded SWF; former head (Jose Filomeno dos Santos, ex-president's son) convicted of fraud"},
        ],
        "data_sources": ["public_reports"],
        "credibility": "inferred",
        "motivation_model": "institutional_mandate",
    },

    # ══════════════════════════════════════════════════════════════════════
    # GLOBAL EXPANSION — LATIN AMERICA
    # ══════════════════════════════════════════════════════════════════════
    "latam_carlos_slim": {
        "name": "Carlos Slim Helu",
        "tier": "institutional",
        "category": "corporation",
        "title": "Chairman, Grupo Carso / America Movil — Latin America's Richest",
        "net_worth_estimate": 93_000_000_000,
        "influence_score": 0.90,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Was world's richest 2010-2013; America Movil: 300M+ mobile subscribers across Latin America; controls ~60% of Mexico's mobile market; real estate, banking, industrial"},
        ],
        "known_positions": [
            {"sector": "telecom", "type": "controlling_stake", "note": "America Movil: Latin America's dominant telecom (Telcel Mexico, Claro across LatAm)"},
            {"sector": "construction", "type": "controlling_stake", "note": "Grupo Carso: Ideal infrastructure, Condumex"},
            {"sector": "retail", "type": "controlling_stake", "note": "Sanborns, Sears Mexico"},
            {"sector": "banking", "type": "controlling_stake", "note": "Grupo Financiero Inbursa"},
            {"sector": "real_estate", "type": "major_stake", "note": "NYC real estate + Mexican properties"},
        ],
        "data_sources": ["bmv_filings", "sec_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "empire_builder",
    },
    "latam_jorge_lemann": {
        "name": "Jorge Paulo Lemann",
        "tier": "institutional",
        "category": "fund",
        "title": "Co-Founder, 3G Capital — Burger King, AB InBev, Kraft Heinz",
        "net_worth_estimate": 16_000_000_000,
        "influence_score": 0.82,
        "connections": [
            {"actor": "ind_buffett", "type": "investment_partner"},
        ],
        "political_affiliations": [
            {"party": "independent", "note": "Brazilian-Swiss; 3G Capital's zero-based budgeting model; partnered with Buffett on Kraft Heinz ($36B deal); AB InBev = world's largest brewer"},
        ],
        "known_positions": [
            {"sector": "beverages", "type": "controlling_stake", "note": "AB InBev: world's largest brewer (Budweiser, Stella Artois, Corona)"},
            {"sector": "food", "type": "controlling_stake", "note": "Kraft Heinz: major food company (with Berkshire Hathaway)"},
            {"sector": "restaurants", "type": "controlling_stake", "note": "Burger King, Tim Hortons, Popeyes (via Restaurant Brands International)"},
        ],
        "data_sources": ["sec_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "profit_maximizer",
    },
    "latam_eduardo_saverin": {
        "name": "Eduardo Saverin",
        "tier": "individual",
        "category": "fund",
        "title": "Facebook Co-Founder; Founder, B Capital Group (Singapore-based)",
        "net_worth_estimate": 22_000_000_000,
        "influence_score": 0.62,
        "connections": [
            {"actor": "ind_zuckerberg", "type": "co_founder"},
            {"actor": "swf_temasek", "type": "investment_partner"},
        ],
        "political_affiliations": [
            {"party": "independent", "note": "Brazilian-born; renounced US citizenship (2012, moved to Singapore); B Capital Group: $6B+ VC/growth equity; Southeast Asia tech investor"},
        ],
        "known_positions": [
            {"sector": "technology", "type": "founder_stake", "note": "Meta/Facebook co-founder; ~2% stake"},
            {"sector": "VC", "type": "controlling_stake", "note": "B Capital Group: growth-stage VC ($6B+) focused on fintech, health tech, logistics"},
        ],
        "data_sources": ["sec_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "tech_investor",
    },
    "latam_marcos_galperin": {
        "name": "Marcos Galperin",
        "tier": "individual",
        "category": "corporation",
        "title": "Founder & CEO, Mercado Libre — Latin America's Amazon",
        "net_worth_estimate": 9_000_000_000,
        "influence_score": 0.68,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Argentine, lives in Uruguay; Mercado Libre = LatAm's largest e-commerce + fintech (Mercado Pago); $80B+ market cap"},
        ],
        "known_positions": [
            {"sector": "ecommerce", "type": "controlling_stake", "note": "Mercado Libre: Latin America's largest e-commerce platform (800M+ users)"},
            {"sector": "fintech", "type": "controlling_stake", "note": "Mercado Pago: LatAm's largest digital payments platform"},
        ],
        "data_sources": ["sec_filings", "nasdaq_filings", "public_reports", "forbes"],
        "credibility": "hard_data",
        "motivation_model": "tech_visionary",
    },
    "latam_vale_mining": {
        "name": "Vale S.A.",
        "tier": "institutional",
        "category": "corporation",
        "title": "Vale — World's Largest Iron Ore Producer (Brazil)",
        "aum": 60_000_000_000,  # market cap
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "independent", "note": "Privatized 1997; world's largest iron ore producer; Brumadinho dam disaster (2019, 270 dead); major nickel producer; critical to China steel supply"},
        ],
        "known_positions": [
            {"sector": "mining", "type": "corporate_leadership", "note": "World's largest iron ore producer; #2 nickel; copper expansion"},
            {"sector": "commodities", "type": "market_moving", "note": "Vale production = China steel supply chain; price-setter for iron ore"},
        ],
        "data_sources": ["sec_filings", "b3_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "latam_pemex": {
        "name": "Pemex (Petroleos Mexicanos)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Pemex — Mexico's State Oil Company (most indebted oil company globally)",
        "aum": 40_000_000_000,
        "influence_score": 0.72,
        "connections": [],
        "political_affiliations": [
            {"party": "Mexican_government", "note": "State-owned; $100B+ debt (world's most indebted oil company); AMLO poured billions in; Dos Bocas refinery boondoggle; national pride asset"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "state_owned", "note": "Mexico's state oil monopoly; 1.6M bpd production; Deer Park refinery (Texas)"},
        ],
        "data_sources": ["bmv_filings", "sec_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "state_directed",
    },
    "latam_petrobras": {
        "name": "Petrobras (Petroleo Brasileiro S.A.)",
        "tier": "institutional",
        "category": "corporation",
        "title": "Petrobras — Brazil's State Oil Giant",
        "aum": 100_000_000_000,  # market cap
        "influence_score": 0.78,
        "connections": [
            {"actor": "swf_bndes", "type": "state_linkage"},
        ],
        "political_affiliations": [
            {"party": "Brazilian_government", "note": "State-controlled; Lava Jato corruption scandal ($5B+ bribes); pre-salt deepwater reserves; 2.7M bpd production; major global oil producer"},
        ],
        "known_positions": [
            {"sector": "energy", "type": "state_controlled", "note": "Petrobras: Brazil's largest company; deepwater pre-salt oil; $90B+ revenue"},
        ],
        "data_sources": ["sec_filings", "b3_filings", "public_reports"],
        "credibility": "hard_data",
        "motivation_model": "state_directed",
    },

    # ══════════════════════════════════════════════════════════════════════
    # GLOBAL EXPANSION — ADDITIONAL CENTRAL BANKS & MONETARY AUTHORITIES
    # ══════════════════════════════════════════════════════════════════════
    "cb_banxico_rodriguez": {
        "name": "Victoria Rodriguez Ceja",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Banco de Mexico (Banxico)",
        "influence_score": 0.72,
        "connections": [],
        "data_sources": ["banxico_decisions", "banxico_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "cb_bcb_neto": {
        "name": "Gabriel Galipolo",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Banco Central do Brasil",
        "influence_score": 0.72,
        "connections": [],
        "data_sources": ["bcb_decisions"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "cb_tcmb_karahan": {
        "name": "Fatih Karahan",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Central Bank of Turkey (TCMB)",
        "influence_score": 0.70,
        "connections": [
            {"actor": "tr_erdogan", "type": "controlled_by"},
        ],
        "political_affiliations": [
            {"party": "AKP_aligned", "note": "Turkey's CB has had 5 governors in 5 years; Erdogan interference; rates went from 8.5% to 50%"},
        ],
        "data_sources": ["tcmb_decisions"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "cb_sarb_kganyago": {
        "name": "Lesetja Kganyago",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, South African Reserve Bank",
        "influence_score": 0.62,
        "connections": [],
        "data_sources": ["sarb_decisions"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "cb_rba_bullock": {
        "name": "Michele Bullock",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Reserve Bank of Australia",
        "influence_score": 0.68,
        "connections": [],
        "data_sources": ["rba_decisions", "rba_speeches"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "cb_bi_warjiyo": {
        "name": "Perry Warjiyo",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Governor, Bank Indonesia",
        "influence_score": 0.62,
        "connections": [],
        "data_sources": ["bi_decisions"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },
    "cb_mas_menon": {
        "name": "Ravi Menon",
        "tier": "sovereign",
        "category": "central_bank",
        "title": "Former Managing Director, Monetary Authority of Singapore (MAS)",
        "influence_score": 0.68,
        "connections": [
            {"actor": "swf_gic", "type": "institutional_peer"},
            {"actor": "swf_temasek", "type": "institutional_peer"},
        ],
        "data_sources": ["mas_reports"],
        "credibility": "hard_data",
        "motivation_model": "institutional_mandate",
    },

    # ══════════════════════════════════════════════════════════════════════
    # US DEEP MAP — STATE PENSION FUNDS (50 states coverage, top 25+ by AUM)
    # The gatekeepers of American retirement savings. Where they allocate
    # determines which PE/hedge funds get capital. Board composition
    # reveals political influence over trillions.
    # ══════════════════════════════════════════════════════════════════════
    "pension_calpers": {
        "name": "California Public Employees' Retirement System (CalPERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Largest US public pension — $558B AUM",
        "aum": 558_000_000_000,
        "influence_score": 0.95,
        "metadata": {
            "cio": "Stephen Gilmore",
            "alternatives_allocation_pct": 33,
            "funded_ratio": 0.75,
            "political_appointees_on_board": 6,
            "state": "CA",
            "members": 2_200_000,
        },
        "connections": [
            {"actor": "pc_blackstone", "type": "LP_relationship"},
            {"actor": "pc_apollo", "type": "LP_relationship"},
            {"actor": "am_fink", "type": "LP_relationship"},
        ],
        "data_sources": ["pension_disclosures", "board_minutes", "calpers_annual_report"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_calstrs": {
        "name": "California State Teachers' Retirement System (CalSTRS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Largest educator-only pension — $397B AUM",
        "aum": 397_000_000_000,
        "influence_score": 0.90,
        "metadata": {
            "cio": "Scott Chan",
            "alternatives_allocation_pct": 30,
            "funded_ratio": 0.72,
            "political_appointees_on_board": 5,
            "state": "CA",
            "members": 1_000_000,
        },
        "data_sources": ["pension_disclosures", "board_minutes", "calstrs_annual_report"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_ny_common": {
        "name": "New York State Common Retirement Fund",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "3rd largest US public pension — $268B AUM",
        "aum": 268_000_000_000,
        "influence_score": 0.88,
        "metadata": {
            "cio": "Anastasia Titarchuk",
            "comptroller": "Thomas DiNapoli",
            "alternatives_allocation_pct": 26,
            "funded_ratio": 0.90,
            "political_appointees_on_board": 1,
            "state": "NY",
        },
        "data_sources": ["pension_disclosures", "osc_annual_report"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_nyc": {
        "name": "New York City Retirement Systems",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "NYC's five pension funds combined — $267B AUM",
        "aum": 267_000_000_000,
        "influence_score": 0.86,
        "metadata": {
            "cio": "Monte Tarbox (interim)",
            "comptroller": "Mark Levine",
            "alternatives_allocation_pct": 24,
            "funded_ratio": 0.78,
            "political_appointees_on_board": 5,
            "state": "NY",
        },
        "data_sources": ["pension_disclosures", "nyc_comptroller_reports"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_florida_sba": {
        "name": "Florida State Board of Administration (SBA)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Florida Retirement System — $215B AUM",
        "aum": 215_000_000_000,
        "influence_score": 0.85,
        "metadata": {
            "cio": "Lamar Taylor",
            "exec_director": "Chris Spencer",
            "alternatives_allocation_pct": 22,
            "funded_ratio": 0.82,
            "political_appointees_on_board": 3,
            "state": "FL",
            "note": "Governor DeSantis pushed anti-ESG mandate",
        },
        "data_sources": ["pension_disclosures", "florida_sba_reports"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_texas_trs": {
        "name": "Teacher Retirement System of Texas",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Texas TRS — $200B AUM",
        "aum": 200_000_000_000,
        "influence_score": 0.84,
        "metadata": {
            "cio": "Jase Auby",
            "alternatives_allocation_pct": 35,
            "funded_ratio": 0.78,
            "political_appointees_on_board": 9,
            "state": "TX",
        },
        "data_sources": ["pension_disclosures", "trs_texas_reports"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_nystrs": {
        "name": "New York State Teachers' Retirement System (NYSTRS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "NYSTRS — $154B AUM",
        "aum": 154_000_000_000,
        "influence_score": 0.80,
        "metadata": {
            "cio": "Thomas Lee",
            "alternatives_allocation_pct": 20,
            "funded_ratio": 0.99,
            "political_appointees_on_board": 3,
            "state": "NY",
        },
        "data_sources": ["pension_disclosures", "nystrs_annual_report"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_wisconsin_swib": {
        "name": "State of Wisconsin Investment Board (SWIB)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Wisconsin pension — $155B AUM",
        "aum": 155_000_000_000,
        "influence_score": 0.78,
        "metadata": {
            "alternatives_allocation_pct": 28,
            "funded_ratio": 0.96,
            "political_appointees_on_board": 6,
            "state": "WI",
            "note": "One of the best-funded in the US",
        },
        "data_sources": ["pension_disclosures", "swib_reports"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_washington_sib": {
        "name": "Washington State Investment Board (WSIB)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Washington pension — $140B AUM",
        "aum": 140_000_000_000,
        "influence_score": 0.77,
        "metadata": {
            "alternatives_allocation_pct": 55,
            "funded_ratio": 0.85,
            "political_appointees_on_board": 5,
            "state": "WA",
            "note": "One of highest alternatives allocations in US",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_ohio_strs": {
        "name": "State Teachers Retirement System of Ohio (STRS Ohio)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "STRS Ohio — $105B AUM",
        "aum": 105_000_000_000,
        "influence_score": 0.76,
        "metadata": {
            "cio": "Aaron DiCenzo",
            "alternatives_allocation_pct": 18,
            "funded_ratio": 0.80,
            "political_appointees_on_board": 5,
            "state": "OH",
            "note": "Board takeover controversy 2024-2025",
        },
        "data_sources": ["pension_disclosures", "strs_ohio_reports"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_ohio_pers": {
        "name": "Ohio Public Employees Retirement System (OPERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "OPERS — $120B AUM",
        "aum": 120_000_000_000,
        "influence_score": 0.76,
        "metadata": {
            "alternatives_allocation_pct": 20,
            "funded_ratio": 0.82,
            "political_appointees_on_board": 5,
            "state": "OH",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_north_carolina": {
        "name": "North Carolina Retirement Systems",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "NC pension — $115B AUM",
        "aum": 115_000_000_000,
        "influence_score": 0.75,
        "metadata": {
            "alternatives_allocation_pct": 15,
            "funded_ratio": 0.88,
            "political_appointees_on_board": 4,
            "state": "NC",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_massachusetts_prim": {
        "name": "Massachusetts Pension Reserves Investment Management (PRIM)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Mass PRIM — $105B AUM",
        "aum": 105_000_000_000,
        "influence_score": 0.76,
        "metadata": {
            "alternatives_allocation_pct": 30,
            "funded_ratio": 0.70,
            "political_appointees_on_board": 4,
            "state": "MA",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_new_jersey": {
        "name": "New Jersey Division of Investment",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "New Jersey pension — $102B AUM",
        "aum": 102_000_000_000,
        "influence_score": 0.74,
        "metadata": {
            "alternatives_allocation_pct": 28,
            "funded_ratio": 0.52,
            "political_appointees_on_board": 6,
            "state": "NJ",
            "note": "Chronically underfunded — political football",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_virginia_rs": {
        "name": "Virginia Retirement System (VRS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Virginia pension — $100B AUM",
        "aum": 100_000_000_000,
        "influence_score": 0.73,
        "metadata": {
            "alternatives_allocation_pct": 22,
            "funded_ratio": 0.82,
            "political_appointees_on_board": 5,
            "state": "VA",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_georgia": {
        "name": "Teachers Retirement System of Georgia",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Georgia TRS — $95B AUM",
        "aum": 95_000_000_000,
        "influence_score": 0.72,
        "metadata": {
            "alternatives_allocation_pct": 18,
            "funded_ratio": 0.77,
            "political_appointees_on_board": 5,
            "state": "GA",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_oregon": {
        "name": "Oregon Public Employees Retirement Fund",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Oregon pension — $95B AUM",
        "aum": 95_000_000_000,
        "influence_score": 0.73,
        "metadata": {
            "alternatives_allocation_pct": 58,
            "funded_ratio": 0.80,
            "political_appointees_on_board": 5,
            "state": "OR",
            "note": "Highest alternatives allocation among US pensions",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_minnesota_sbi": {
        "name": "Minnesota State Board of Investment",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Minnesota pension — $90B AUM",
        "aum": 90_000_000_000,
        "influence_score": 0.72,
        "metadata": {
            "alternatives_allocation_pct": 22,
            "funded_ratio": 0.80,
            "political_appointees_on_board": 4,
            "state": "MN",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_michigan": {
        "name": "Michigan Retirement Systems",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Michigan pension — $88B AUM",
        "aum": 88_000_000_000,
        "influence_score": 0.72,
        "metadata": {
            "alternatives_allocation_pct": 25,
            "funded_ratio": 0.72,
            "political_appointees_on_board": 4,
            "state": "MI",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_uc_regents": {
        "name": "University of California Retirement System",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "UC Retirement — $85B AUM",
        "aum": 85_000_000_000,
        "influence_score": 0.73,
        "metadata": {
            "alternatives_allocation_pct": 25,
            "funded_ratio": 0.84,
            "political_appointees_on_board": 7,
            "state": "CA",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_alaska": {
        "name": "Alaska Permanent Fund Corporation",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Alaska Permanent Fund — $78B AUM",
        "aum": 78_000_000_000,
        "influence_score": 0.72,
        "metadata": {
            "alternatives_allocation_pct": 20,
            "funded_ratio": 1.0,
            "political_appointees_on_board": 6,
            "state": "AK",
            "note": "Sovereign-wealth-like fund; pays annual dividend to residents",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_la_county_lacera": {
        "name": "Los Angeles County Employees Retirement Association (LACERA)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "LACERA — $78B AUM",
        "aum": 78_000_000_000,
        "influence_score": 0.72,
        "metadata": {
            "alternatives_allocation_pct": 22,
            "funded_ratio": 0.74,
            "political_appointees_on_board": 4,
            "state": "CA",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_pennsylvania_psers": {
        "name": "Pennsylvania Public School Employees' Retirement System (PSERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "PA PSERS — $75B AUM",
        "aum": 75_000_000_000,
        "influence_score": 0.72,
        "metadata": {
            "alternatives_allocation_pct": 30,
            "funded_ratio": 0.58,
            "political_appointees_on_board": 7,
            "state": "PA",
            "note": "FBI investigation into performance calculation errors",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_maryland": {
        "name": "Maryland State Retirement and Pension System",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Maryland pension — $68B AUM",
        "aum": 68_000_000_000,
        "influence_score": 0.70,
        "metadata": {
            "alternatives_allocation_pct": 25,
            "funded_ratio": 0.72,
            "political_appointees_on_board": 6,
            "state": "MD",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_illinois_trs": {
        "name": "Teachers' Retirement System of Illinois",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Illinois TRS — $65B AUM",
        "aum": 65_000_000_000,
        "influence_score": 0.70,
        "metadata": {
            "alternatives_allocation_pct": 18,
            "funded_ratio": 0.44,
            "political_appointees_on_board": 6,
            "state": "IL",
            "note": "Severely underfunded — IL pension crisis",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_tennessee": {
        "name": "Tennessee Consolidated Retirement System",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Tennessee pension — $65B AUM",
        "aum": 65_000_000_000,
        "influence_score": 0.68,
        "metadata": {
            "alternatives_allocation_pct": 18,
            "funded_ratio": 0.92,
            "political_appointees_on_board": 5,
            "state": "TN",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_colorado_pera": {
        "name": "Colorado Public Employees' Retirement Association (PERA)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Colorado PERA — $60B AUM",
        "aum": 60_000_000_000,
        "influence_score": 0.68,
        "metadata": {
            "alternatives_allocation_pct": 18,
            "funded_ratio": 0.68,
            "political_appointees_on_board": 5,
            "state": "CO",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_nevada": {
        "name": "Public Employees' Retirement System of Nevada",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Nevada pension — $55B AUM",
        "aum": 55_000_000_000,
        "influence_score": 0.66,
        "metadata": {
            "alternatives_allocation_pct": 14,
            "funded_ratio": 0.77,
            "political_appointees_on_board": 4,
            "state": "NV",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_arizona": {
        "name": "Arizona State Retirement System (ASRS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Arizona pension — $48B AUM",
        "aum": 48_000_000_000,
        "influence_score": 0.66,
        "metadata": {
            "alternatives_allocation_pct": 20,
            "funded_ratio": 0.74,
            "political_appointees_on_board": 4,
            "state": "AZ",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_indiana": {
        "name": "Indiana Public Retirement System (INPRS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Indiana pension — $46B AUM",
        "aum": 46_000_000_000,
        "influence_score": 0.66,
        "metadata": {
            "alternatives_allocation_pct": 18,
            "funded_ratio": 0.85,
            "political_appointees_on_board": 5,
            "state": "IN",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_connecticut": {
        "name": "Connecticut Retirement Plans and Trust Funds",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Connecticut pension — $45B AUM",
        "aum": 45_000_000_000,
        "influence_score": 0.66,
        "metadata": {
            "alternatives_allocation_pct": 22,
            "funded_ratio": 0.52,
            "political_appointees_on_board": 5,
            "state": "CT",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_alabama": {
        "name": "Retirement Systems of Alabama (RSA)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Alabama pension — $45B AUM",
        "aum": 45_000_000_000,
        "influence_score": 0.65,
        "metadata": {
            "cio": "David Bronner (CEO/Secretary-Treasurer)",
            "alternatives_allocation_pct": 12,
            "funded_ratio": 0.70,
            "political_appointees_on_board": 5,
            "state": "AL",
            "note": "RSA owns Robert Trent Jones Golf Trail, newspapers, and office buildings",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_iowa": {
        "name": "Iowa Public Employees' Retirement System (IPERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Iowa pension — $42B AUM",
        "aum": 42_000_000_000,
        "influence_score": 0.64,
        "metadata": {
            "alternatives_allocation_pct": 15,
            "funded_ratio": 0.86,
            "political_appointees_on_board": 3,
            "state": "IA",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_utah": {
        "name": "Utah Retirement Systems",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Utah pension — $42B AUM",
        "aum": 42_000_000_000,
        "influence_score": 0.64,
        "metadata": {
            "alternatives_allocation_pct": 16,
            "funded_ratio": 0.92,
            "political_appointees_on_board": 4,
            "state": "UT",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_south_carolina": {
        "name": "South Carolina Retirement System Investment Commission",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "SC pension — $40B AUM",
        "aum": 40_000_000_000,
        "influence_score": 0.65,
        "metadata": {
            "alternatives_allocation_pct": 20,
            "funded_ratio": 0.57,
            "political_appointees_on_board": 5,
            "state": "SC",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_texas_ers": {
        "name": "Employees Retirement System of Texas",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Texas ERS — $38B AUM",
        "aum": 38_000_000_000,
        "influence_score": 0.68,
        "metadata": {
            "alternatives_allocation_pct": 20,
            "funded_ratio": 0.65,
            "political_appointees_on_board": 6,
            "state": "TX",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_mississippi": {
        "name": "Public Employees' Retirement System of Mississippi (PERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Mississippi PERS — $32B AUM",
        "aum": 32_000_000_000,
        "influence_score": 0.60,
        "metadata": {
            "alternatives_allocation_pct": 16,
            "funded_ratio": 0.60,
            "political_appointees_on_board": 5,
            "state": "MS",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_kansas": {
        "name": "Kansas Public Employees Retirement System (KPERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Kansas KPERS — $26B AUM",
        "aum": 26_000_000_000,
        "influence_score": 0.58,
        "metadata": {
            "alternatives_allocation_pct": 14,
            "funded_ratio": 0.72,
            "political_appointees_on_board": 5,
            "state": "KS",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_nebraska": {
        "name": "Nebraska Investment Council",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Nebraska pension — $25B AUM",
        "aum": 25_000_000_000,
        "influence_score": 0.58,
        "metadata": {
            "alternatives_allocation_pct": 14,
            "funded_ratio": 0.90,
            "political_appointees_on_board": 5,
            "state": "NE",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_idaho": {
        "name": "Public Employee Retirement System of Idaho (PERSI)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Idaho pension — $24B AUM",
        "aum": 24_000_000_000,
        "influence_score": 0.57,
        "metadata": {
            "alternatives_allocation_pct": 12,
            "funded_ratio": 0.93,
            "political_appointees_on_board": 4,
            "state": "ID",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_kentucky": {
        "name": "Kentucky Retirement Systems",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Kentucky pension — $22B AUM",
        "aum": 22_000_000_000,
        "influence_score": 0.62,
        "metadata": {
            "alternatives_allocation_pct": 14,
            "funded_ratio": 0.44,
            "political_appointees_on_board": 6,
            "state": "KY",
            "note": "Among worst-funded in the US",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_hawaii": {
        "name": "Employees' Retirement System of the State of Hawaii",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Hawaii pension — $22B AUM",
        "aum": 22_000_000_000,
        "influence_score": 0.58,
        "metadata": {
            "alternatives_allocation_pct": 16,
            "funded_ratio": 0.55,
            "political_appointees_on_board": 4,
            "state": "HI",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_arkansas": {
        "name": "Arkansas Teacher Retirement System",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Arkansas TRS — $20B AUM",
        "aum": 20_000_000_000,
        "influence_score": 0.56,
        "metadata": {
            "alternatives_allocation_pct": 12,
            "funded_ratio": 0.75,
            "political_appointees_on_board": 5,
            "state": "AR",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_west_virginia": {
        "name": "West Virginia Investment Management Board",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "West Virginia pension — $20B AUM",
        "aum": 20_000_000_000,
        "influence_score": 0.56,
        "metadata": {
            "alternatives_allocation_pct": 14,
            "funded_ratio": 0.82,
            "political_appointees_on_board": 5,
            "state": "WV",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_maine": {
        "name": "Maine Public Employees Retirement System (MainePERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Maine pension — $19B AUM",
        "aum": 19_000_000_000,
        "influence_score": 0.56,
        "metadata": {
            "alternatives_allocation_pct": 14,
            "funded_ratio": 0.82,
            "political_appointees_on_board": 4,
            "state": "ME",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_new_mexico": {
        "name": "New Mexico Public Employees Retirement Association (PERA)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "New Mexico PERA — $18B AUM",
        "aum": 18_000_000_000,
        "influence_score": 0.58,
        "metadata": {
            "alternatives_allocation_pct": 16,
            "funded_ratio": 0.70,
            "political_appointees_on_board": 5,
            "state": "NM",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_south_dakota": {
        "name": "South Dakota Retirement System (SDRS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "South Dakota pension — $15B AUM",
        "aum": 15_000_000_000,
        "influence_score": 0.56,
        "metadata": {
            "alternatives_allocation_pct": 12,
            "funded_ratio": 1.00,
            "political_appointees_on_board": 4,
            "state": "SD",
            "note": "Fully funded — model pension",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_louisiana": {
        "name": "Louisiana State Employees' Retirement System (LASERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Louisiana LASERS — $14B AUM",
        "aum": 14_000_000_000,
        "influence_score": 0.56,
        "metadata": {
            "alternatives_allocation_pct": 14,
            "funded_ratio": 0.66,
            "political_appointees_on_board": 5,
            "state": "LA",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_montana": {
        "name": "Montana Public Employee Retirement Administration (MPERA)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Montana pension — $14B AUM",
        "aum": 14_000_000_000,
        "influence_score": 0.54,
        "metadata": {
            "alternatives_allocation_pct": 10,
            "funded_ratio": 0.74,
            "political_appointees_on_board": 4,
            "state": "MT",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_missouri": {
        "name": "Missouri State Employees' Retirement System (MOSERS)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Missouri pension — $12B AUM",
        "aum": 12_000_000_000,
        "influence_score": 0.60,
        "metadata": {
            "alternatives_allocation_pct": 15,
            "funded_ratio": 0.82,
            "political_appointees_on_board": 5,
            "state": "MO",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_delaware": {
        "name": "Delaware Public Employees' Retirement System",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Delaware pension — $12B AUM",
        "aum": 12_000_000_000,
        "influence_score": 0.55,
        "metadata": {
            "alternatives_allocation_pct": 14,
            "funded_ratio": 0.85,
            "political_appointees_on_board": 4,
            "state": "DE",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_rhode_island": {
        "name": "Employees' Retirement System of Rhode Island",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Rhode Island pension — $11B AUM",
        "aum": 11_000_000_000,
        "influence_score": 0.54,
        "metadata": {
            "alternatives_allocation_pct": 22,
            "funded_ratio": 0.60,
            "political_appointees_on_board": 5,
            "state": "RI",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_wyoming": {
        "name": "Wyoming Retirement System",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Wyoming pension — $10B AUM",
        "aum": 10_000_000_000,
        "influence_score": 0.50,
        "metadata": {
            "alternatives_allocation_pct": 10,
            "funded_ratio": 0.80,
            "political_appointees_on_board": 3,
            "state": "WY",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_north_dakota": {
        "name": "North Dakota Retirement and Investment Office (RIO)",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "North Dakota pension — $8B AUM",
        "aum": 8_000_000_000,
        "influence_score": 0.50,
        "metadata": {
            "alternatives_allocation_pct": 10,
            "funded_ratio": 0.72,
            "political_appointees_on_board": 3,
            "state": "ND",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },
    "pension_vermont": {
        "name": "Vermont Pension Investment Commission",
        "tier": "institutional",
        "category": "pension_fund",
        "title": "Vermont pension — $6B AUM",
        "aum": 6_000_000_000,
        "influence_score": 0.48,
        "metadata": {
            "alternatives_allocation_pct": 12,
            "funded_ratio": 0.68,
            "political_appointees_on_board": 4,
            "state": "VT",
        },
        "data_sources": ["pension_disclosures"],
        "credibility": "hard_data",
        "motivation_model": "fiduciary_mandate",
    },

    # ══════════════════════════════════════════════════════════════════════
    # TOP LOBBYING FIRMS — by 2024-2025 revenue
    # The transmission belt between corporate money and government policy.
    # ══════════════════════════════════════════════════════════════════════
    "lobby_ballard_partners": {"name": "Ballard Partners", "tier": "institutional", "category": "lobbying_firm", "title": "Top-earning lobbying firm 2025 — $88.1M revenue", "influence_score": 0.92, "metadata": {"revenue_2025": 88_100_000, "founder": "Brian Ballard", "key_lobbyists": ["Brian Ballard", "Susie Wiles (former)"], "top_clients": ["Meta", "Amazon", "Uber", "Publix"], "political_connections": "Trump-aligned; Susie Wiles went from firm to WH Chief of Staff", "revolving_door": True}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_brownstein": {"name": "Brownstein Hyatt Farber Schreck", "tier": "institutional", "category": "lobbying_firm", "title": "#1 lobbying firm 2020-2024 — $67.9M (2024)", "influence_score": 0.90, "metadata": {"revenue_2024": 67_900_000, "revenue_2025": 70_000_000, "founder": "Norman Brownstein", "key_lobbyists": ["Marc Lampkin", "Al Mottur"], "top_clients": ["Google", "Comcast", "Airbnb"], "political_connections": "Bipartisan powerhouse", "revolving_door": True}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_bgr_group": {"name": "BGR Group", "tier": "institutional", "category": "lobbying_firm", "title": "BGR Group — $71.5M revenue (2025)", "influence_score": 0.88, "metadata": {"revenue_2025": 71_500_000, "founder": "Haley Barbour (former MS Governor/RNC Chair)", "key_lobbyists": ["Ed Rogers", "Haley Barbour"], "top_clients": ["Saudi Arabia", "Raytheon", "Boeing", "PhRMA"], "political_connections": "Republican establishment", "revolving_door": True}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_akin_gump": {"name": "Akin Gump Strauss Hauer & Feld", "tier": "institutional", "category": "lobbying_firm", "title": "Akin Gump — $65.3M (2025)", "influence_score": 0.89, "metadata": {"revenue_2025": 65_300_000, "founder": "Robert Strauss (former DNC Chair)", "key_lobbyists": ["Brian Pomper", "Vic Fazio"], "top_clients": ["AT&T", "Lockheed Martin", "Koch Industries"], "political_connections": "Bipartisan; Democratic roots", "revolving_door": True}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_holland_knight": {"name": "Holland & Knight", "tier": "institutional", "category": "lobbying_firm", "title": "Holland & Knight — $54.6M (2025)", "influence_score": 0.86, "metadata": {"revenue_2025": 54_600_000, "key_lobbyists": ["Rich Gold", "David Tamasi"], "top_clients": ["Northrop Grumman", "Google", "Walmart"], "political_connections": "Bipartisan; defense and tech", "revolving_door": True}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_cornerstone": {"name": "Cornerstone Government Affairs", "tier": "institutional", "category": "lobbying_firm", "title": "Cornerstone — $48.6M (2025)", "influence_score": 0.84, "metadata": {"revenue_2025": 48_600_000, "founder": "Rogers Johnson", "top_clients": ["infrastructure", "agriculture", "333 clients"], "political_connections": "Bipartisan; appropriations focus", "revolving_door": True}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_invariant": {"name": "Invariant LLC", "tier": "institutional", "category": "lobbying_firm", "title": "Invariant — $42.3M (2025)", "influence_score": 0.83, "metadata": {"revenue_2025": 42_300_000, "founder": "Heather Podesta", "top_clients": ["AI/tech", "pharma", "crypto"], "political_connections": "Democratic-aligned; Podesta power broker", "revolving_door": True, "note": "AI oversight, data privacy, semiconductor lobbying"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_squire_patton": {"name": "Squire Patton Boggs", "tier": "institutional", "category": "lobbying_firm", "title": "Squire Patton Boggs — $23.1M (2025)", "influence_score": 0.80, "metadata": {"revenue_2025": 23_100_000, "key_lobbyists": ["Jack Kingston (former Rep)", "Trent Lott (former Senate Majority Leader)"], "top_clients": ["defense", "healthcare", "trade"], "political_connections": "Bipartisan; heavy revolving door", "revolving_door": True}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_checkmate": {"name": "Checkmate Government Relations", "tier": "institutional", "category": "lobbying_firm", "title": "Checkmate — $70K to $22.2M in one year (2025)", "influence_score": 0.78, "metadata": {"revenue_2025": 22_200_000, "revenue_2024": 70_000, "political_connections": "Trump-aligned; explosive growth post-2024 election", "revolving_door": True, "note": "Quintessential revolving door — from $70K to $22.2M"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_forbes_tate": {"name": "Forbes Tate Partners", "tier": "institutional", "category": "lobbying_firm", "title": "Forbes Tate — top 10 firm", "influence_score": 0.78, "metadata": {"revenue_2024": 38_000_000, "founder": "Jeff Forbes / Craig Tate", "top_clients": ["pharma", "tech", "energy"], "political_connections": "Bipartisan"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_thorn_run": {"name": "Thorn Run Partners", "tier": "institutional", "category": "lobbying_firm", "title": "Thorn Run — financial services focus", "influence_score": 0.78, "metadata": {"revenue_2024": 36_000_000, "top_clients": ["financial services", "tech"], "political_connections": "Bipartisan"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_mehlman": {"name": "Mehlman Consulting", "tier": "institutional", "category": "lobbying_firm", "title": "Mehlman Consulting — tech/telecom", "influence_score": 0.77, "metadata": {"revenue_2024": 30_000_000, "founder": "Bruce Mehlman (former Commerce Dept)", "top_clients": ["tech", "telecom"], "political_connections": "Republican"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_cassidy": {"name": "Cassidy & Associates", "tier": "institutional", "category": "lobbying_firm", "title": "Cassidy — pioneer of earmark lobbying", "influence_score": 0.76, "metadata": {"revenue_2024": 28_000_000, "founder": "Gerald Cassidy", "top_clients": ["defense", "higher education"], "political_connections": "Bipartisan"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_capitol_counsel": {"name": "Capitol Counsel", "tier": "institutional", "category": "lobbying_firm", "title": "Capitol Counsel — bipartisan D.C. firm", "influence_score": 0.75, "metadata": {"revenue_2024": 25_000_000, "top_clients": ["pharma", "defense", "energy"], "political_connections": "Bipartisan"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_covington": {"name": "Covington & Burling LLP", "tier": "institutional", "category": "lobbying_firm", "title": "Covington — elite law firm lobbying (Eric Holder was partner)", "influence_score": 0.78, "metadata": {"revenue_2024": 22_000_000, "top_clients": ["pharma", "tech", "trade"], "political_connections": "Bipartisan; Holder connection"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_crossroads": {"name": "Crossroads Strategies", "tier": "institutional", "category": "lobbying_firm", "title": "Crossroads — bipartisan", "influence_score": 0.74, "metadata": {"revenue_2024": 22_000_000, "top_clients": ["energy", "finance", "healthcare"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_k_l_gates": {"name": "K&L Gates Public Policy Practice", "tier": "institutional", "category": "lobbying_firm", "title": "K&L Gates lobbying arm", "influence_score": 0.74, "metadata": {"revenue_2024": 20_000_000, "top_clients": ["energy", "finance"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_podesta": {"name": "Podesta Group (legacy) / Cogent Strategies", "tier": "institutional", "category": "lobbying_firm", "title": "Podesta Group — Democratic influence machine", "influence_score": 0.76, "metadata": {"founder": "Tony Podesta", "top_clients": ["tech", "defense", "foreign governments"], "political_connections": "Tony Podesta brother of John Podesta (WH Climate Envoy)"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_arnold_porter": {"name": "Arnold & Porter Kaye Scholer", "tier": "institutional", "category": "lobbying_firm", "title": "Arnold & Porter — D.C. powerhouse", "influence_score": 0.76, "metadata": {"revenue_2024": 19_000_000, "top_clients": ["finance", "healthcare", "insurance"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_williams_jensen": {"name": "Williams & Jensen", "tier": "institutional", "category": "lobbying_firm", "title": "Williams & Jensen — tax/finance specialists", "influence_score": 0.74, "metadata": {"revenue_2024": 18_000_000, "top_clients": ["finance", "insurance", "tax policy"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_hogan_lovells": {"name": "Hogan Lovells US LLP", "tier": "institutional", "category": "lobbying_firm", "title": "Hogan Lovells — regulatory lobbying", "influence_score": 0.75, "metadata": {"revenue_2024": 17_000_000, "top_clients": ["pharma", "tech", "defense"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_ogilvy": {"name": "Ogilvy Government Relations", "tier": "institutional", "category": "lobbying_firm", "title": "Ogilvy GR — WPP-owned", "influence_score": 0.73, "metadata": {"revenue_2024": 16_000_000, "top_clients": ["pharma", "tech", "consumer"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_skadden": {"name": "Skadden, Arps (Political Law)", "tier": "institutional", "category": "lobbying_firm", "title": "Skadden — CFIUS/sanctions/regulatory", "influence_score": 0.76, "metadata": {"revenue_2024": 16_000_000, "top_clients": ["M&A regulatory", "CFIUS", "sanctions"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_targeted_victory": {"name": "Targeted Victory", "tier": "institutional", "category": "lobbying_firm", "title": "Targeted Victory — Republican digital + lobbying", "influence_score": 0.72, "metadata": {"revenue_2024": 15_000_000, "founder": "Zac Moffatt", "top_clients": ["Republican campaigns", "tech", "crypto"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_gibson_dunn": {"name": "Gibson, Dunn & Crutcher", "tier": "institutional", "category": "lobbying_firm", "title": "Gibson Dunn — government affairs", "influence_score": 0.74, "metadata": {"revenue_2024": 15_000_000, "top_clients": ["tech", "finance"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_fti": {"name": "FTI Consulting (Government Affairs)", "tier": "institutional", "category": "lobbying_firm", "title": "FTI — strategic comms + lobbying", "influence_score": 0.74, "metadata": {"revenue_2024": 14_000_000, "top_clients": ["finance", "energy", "restructuring"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_american_continental": {"name": "American Continental Group", "tier": "institutional", "category": "lobbying_firm", "title": "American Continental", "influence_score": 0.72, "metadata": {"revenue_2024": 14_000_000, "top_clients": ["defense", "energy"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_mayer_brown": {"name": "Mayer Brown LLP (Government Affairs)", "tier": "institutional", "category": "lobbying_firm", "title": "Mayer Brown — regulatory", "influence_score": 0.72, "metadata": {"revenue_2024": 14_000_000, "top_clients": ["finance", "trade"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_bracewell": {"name": "Bracewell LLP", "tier": "institutional", "category": "lobbying_firm", "title": "Bracewell — energy sector specialists", "influence_score": 0.72, "metadata": {"revenue_2024": 13_000_000, "top_clients": ["oil & gas", "energy", "utilities"], "political_connections": "Republican; Texas roots"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_tiber_creek": {"name": "Tiber Creek Group", "tier": "institutional", "category": "lobbying_firm", "title": "Tiber Creek — conservative policy", "influence_score": 0.71, "metadata": {"revenue_2024": 12_000_000, "top_clients": ["conservative nonprofits", "energy"], "political_connections": "Heritage Foundation / Federalist Society ties"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_kelley_drye": {"name": "Kelley Drye & Warren", "tier": "institutional", "category": "lobbying_firm", "title": "Kelley Drye — trade/regulatory", "influence_score": 0.70, "metadata": {"revenue_2024": 12_000_000, "top_clients": ["trade", "manufacturing"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_monument": {"name": "Monument Advocacy", "tier": "institutional", "category": "lobbying_firm", "title": "Monument Advocacy", "influence_score": 0.73, "metadata": {"revenue_2024": 18_000_000, "top_clients": ["tech", "defense"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_harbinger": {"name": "Harbinger Strategies", "tier": "institutional", "category": "lobbying_firm", "title": "Harbinger — Republican firm", "influence_score": 0.72, "metadata": {"revenue_2024": 16_000_000, "top_clients": ["energy", "defense"], "political_connections": "Republican; former Romney/McConnell operatives"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_venn": {"name": "Venn Strategies", "tier": "institutional", "category": "lobbying_firm", "title": "Venn — tax policy specialists", "influence_score": 0.70, "metadata": {"revenue_2024": 11_000_000, "top_clients": ["tax policy", "finance"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_prime_policy": {"name": "Prime Policy Group", "tier": "institutional", "category": "lobbying_firm", "title": "Prime Policy — Reagan-era roots", "influence_score": 0.68, "metadata": {"revenue_2024": 9_000_000, "founder": "Charlie Black (Reagan advisor)"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_fierce": {"name": "Fierce Government Relations", "tier": "institutional", "category": "lobbying_firm", "title": "Fierce GR — telecom/tech", "influence_score": 0.68, "metadata": {"revenue_2024": 9_500_000, "top_clients": ["telecom", "broadband"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_peck_madigan": {"name": "Peck Madigan Jones", "tier": "institutional", "category": "lobbying_firm", "title": "Peck Madigan — energy/environment", "influence_score": 0.69, "metadata": {"revenue_2024": 10_000_000, "top_clients": ["energy", "environment"], "political_connections": "Democratic-leaning"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_dutko": {"name": "Dutko Grayling", "tier": "institutional", "category": "lobbying_firm", "title": "Dutko — healthcare/education", "influence_score": 0.68, "metadata": {"revenue_2024": 9_000_000, "top_clients": ["healthcare", "education"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_david_turch": {"name": "David Turch & Associates", "tier": "institutional", "category": "lobbying_firm", "title": "Turch — construction/labor lobbying", "influence_score": 0.67, "metadata": {"revenue_2024": 8_000_000, "top_clients": ["construction", "labor unions"], "political_connections": "Democratic-leaning"}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "lobby_navigators": {"name": "Navigators Global", "tier": "institutional", "category": "lobbying_firm", "title": "Navigators — international trade", "influence_score": 0.68, "metadata": {"revenue_2024": 8_000_000, "top_clients": ["international trade", "agriculture"]}, "data_sources": ["opensecrets_lda"], "credibility": "hard_data", "motivation_model": "profit_influence"},

    # ══════════════════════════════════════════════════════════════════════
    # TOP POLITICAL DONORS — 2024 election cycle
    # Who buys American policy. $2.5B+ from top 50 donors alone.
    # ══════════════════════════════════════════════════════════════════════
    "donor_musk": {"name": "Elon Musk", "tier": "individual", "category": "political_donor", "title": "#1 donor 2024 — $291M to Republicans", "net_worth_estimate": 240_000_000_000, "influence_score": 0.98, "metadata": {"total_donated_2024": 291_000_000, "party": "Republican", "recipients": ["America PAC", "Trump 47 Committee", "MAGA Inc"], "industry": "tech/auto/space", "wants": "Deregulation, DOGE influence, SpaceX/Tesla contracts, crypto"}, "connections": [{"actor": "ind_musk", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest_ideological"},
    "donor_mellon": {"name": "Timothy Mellon", "tier": "individual", "category": "political_donor", "title": "#2 donor — $150M+ to Republicans", "net_worth_estimate": 14_100_000_000, "influence_score": 0.85, "metadata": {"total_donated_2024": 150_000_000, "party": "Republican", "recipients": ["MAGA Inc", "RFK Jr"], "industry": "banking/railroads heir", "wants": "Immigration enforcement, border wall, deregulation"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_adelson": {"name": "Miriam Adelson", "tier": "individual", "category": "political_donor", "title": "#3 donor — $100M+ to Republicans", "net_worth_estimate": 32_800_000_000, "influence_score": 0.84, "metadata": {"total_donated_2024": 100_000_000, "party": "Republican", "recipients": ["Preserve America PAC"], "industry": "casinos (Las Vegas Sands)", "wants": "Pro-Israel policy, online gambling regulation"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_uihlein": {"name": "Richard & Elizabeth Uihlein", "tier": "individual", "category": "political_donor", "title": "Uihleins — $105M to Republicans", "net_worth_estimate": 8_600_000_000, "influence_score": 0.82, "metadata": {"total_donated_2024": 105_000_000, "party": "Republican", "recipients": ["Restoration PAC", "Club for Growth"], "industry": "shipping supplies (Uline)", "wants": "Deregulation, tax cuts, conservative social policy"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_griffin_ken": {"name": "Kenneth Griffin", "tier": "individual", "category": "political_donor", "title": "Griffin (Citadel) — $100M+ to Republicans", "net_worth_estimate": 43_000_000_000, "influence_score": 0.90, "metadata": {"total_donated_2024": 100_000_000, "party": "Republican", "recipients": ["SLF", "CLF", "various GOP"], "industry": "hedge funds (Citadel)", "wants": "Financial deregulation, low cap gains, favorable market structure"}, "connections": [{"actor": "hf_griffin", "type": "same_entity"}, {"actor": "hf_citadel", "type": "founder"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest_ideological"},
    "donor_yass": {"name": "Jeffrey & Janine Yass", "tier": "individual", "category": "political_donor", "title": "Yass (Susquehanna) — $100M+ to Republicans", "net_worth_estimate": 45_000_000_000, "influence_score": 0.88, "metadata": {"total_donated_2024": 100_000_000, "party": "Republican", "recipients": ["Club for Growth"], "industry": "trading (SIG)", "wants": "School choice, crypto-friendly regulation, opposes TikTok ban (major investor)"}, "connections": [{"actor": "hf_yass", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest_ideological"},
    "donor_singer_paul": {"name": "Paul Singer", "tier": "individual", "category": "political_donor", "title": "Singer (Elliott) — $66.8M to Republicans", "net_worth_estimate": 6_000_000_000, "influence_score": 0.82, "metadata": {"total_donated_2024": 66_800_000, "party": "Republican", "recipients": ["American Unity PAC"], "industry": "hedge funds (Elliott)", "wants": "Activist-friendly governance, sovereign debt enforcement"}, "connections": [{"actor": "hf_singer", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest_ideological"},
    "donor_bloomberg": {"name": "Michael Bloomberg", "tier": "individual", "category": "political_donor", "title": "Bloomberg — $64M to Democrats", "net_worth_estimate": 106_000_000_000, "influence_score": 0.90, "metadata": {"total_donated_2024": 64_000_000, "party": "Democrat", "recipients": ["Independence USA PAC", "Everytown"], "industry": "media/finance (Bloomberg LP)", "wants": "Gun control, climate policy, immigration reform"}, "connections": [{"actor": "pol_bloomberg", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_moskovitz": {"name": "Dustin Moskovitz", "tier": "individual", "category": "political_donor", "title": "Moskovitz (Facebook co-founder) — $51M to Democrats", "net_worth_estimate": 13_000_000_000, "influence_score": 0.78, "metadata": {"total_donated_2024": 51_000_000, "party": "Democrat", "recipients": ["Future Forward USA PAC"], "industry": "tech (Facebook co-founder, Asana)", "wants": "EA-aligned policy, pandemic preparedness, AI safety"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_koch_network": {"name": "Koch Network (Americans for Prosperity)", "tier": "institutional", "category": "political_donor", "title": "Koch Network — $70M+ via AFP", "influence_score": 0.90, "metadata": {"total_donated_2024": 70_000_000, "party": "Republican", "recipients": ["AFP Action", "initially Haley, then Trump"], "industry": "energy/chemicals (Koch Industries)", "wants": "Deregulation, tax cuts, school choice, anti-union"}, "connections": [{"actor": "dynasty_koch", "type": "controls"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_soros_fund": {"name": "Fund for Policy Reform (Soros 501c4)", "tier": "institutional", "category": "political_donor", "title": "Soros dark money — $60M to Democracy PAC", "influence_score": 0.82, "metadata": {"total_donated_2024": 60_000_000, "party": "Democrat", "recipients": ["Democracy PAC"], "industry": "philanthropy", "wants": "Criminal justice reform, voting rights, democracy"}, "connections": [{"actor": "pol_soros_george", "type": "controls"}], "data_sources": ["opensecrets", "irs_990"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_a16z": {"name": "Andreessen Horowitz (a16z)", "tier": "institutional", "category": "political_donor", "title": "a16z — $45.2M to Fairshake (crypto PAC)", "influence_score": 0.84, "metadata": {"total_donated_2024": 45_200_000, "party": "Bipartisan (crypto)", "recipients": ["Fairshake PAC"], "industry": "venture capital", "wants": "Crypto regulation, no SEC overreach, AI deregulation"}, "connections": [{"actor": "pol_andreessen", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},
    "donor_coinbase": {"name": "Coinbase Inc.", "tier": "institutional", "category": "political_donor", "title": "Coinbase — $46.5M to Fairshake PAC", "influence_score": 0.80, "metadata": {"total_donated_2024": 46_500_000, "party": "Bipartisan (crypto)", "recipients": ["Fairshake PAC"], "industry": "crypto exchange", "wants": "Crypto-friendly regulation, stablecoin framework"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},
    "donor_ripple": {"name": "Ripple Labs", "tier": "institutional", "category": "political_donor", "title": "Ripple — $45M to Fairshake", "influence_score": 0.78, "metadata": {"total_donated_2024": 45_000_000, "party": "Bipartisan (crypto)", "recipients": ["Fairshake PAC"], "industry": "crypto/payments", "wants": "XRP not classified as security"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},
    "donor_ricketts": {"name": "Ricketts Family", "tier": "individual", "category": "political_donor", "title": "Ricketts — $35M to Republicans", "net_worth_estimate": 4_000_000_000, "influence_score": 0.76, "metadata": {"total_donated_2024": 35_000_000, "party": "Republican", "recipients": ["Ending Spending"], "industry": "finance (TD Ameritrade founder)", "wants": "Low taxes, deregulation"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_schwarzman": {"name": "Stephen Schwarzman", "tier": "individual", "category": "political_donor", "title": "Schwarzman (Blackstone) — $27M to GOP", "net_worth_estimate": 42_000_000_000, "influence_score": 0.86, "metadata": {"total_donated_2024": 27_000_000, "party": "Republican", "recipients": ["Trump campaign", "various GOP"], "industry": "private equity (Blackstone)", "wants": "Carried interest, low cap gains, PE-friendly regulation"}, "connections": [{"actor": "pe_schwarzman", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest_ideological"},
    "donor_hoffman": {"name": "Reid Hoffman", "tier": "individual", "category": "political_donor", "title": "Hoffman (LinkedIn) — $26M to Democrats", "net_worth_estimate": 2_500_000_000, "influence_score": 0.80, "metadata": {"total_donated_2024": 26_000_000, "party": "Democrat", "recipients": ["various Democratic PACs"], "industry": "tech (LinkedIn, Greylock)", "wants": "Tech-friendly regulation, immigration reform, democracy"}, "connections": [{"actor": "pol_hoffman", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_wyss": {"name": "Hansjorg Wyss", "tier": "individual", "category": "political_donor", "title": "Wyss — $25M via Arabella dark money network", "net_worth_estimate": 7_000_000_000, "influence_score": 0.74, "metadata": {"total_donated_2024": 25_000_000, "party": "Democrat", "recipients": ["Sixteen Thirty Fund", "Arabella Advisors"], "industry": "medical devices (Synthes)", "wants": "Conservation, climate policy", "note": "Swiss national; gives via 501c4 dark money"}, "data_sources": ["opensecrets", "irs_990"], "credibility": "derived", "motivation_model": "ideological"},
    "donor_simons_family": {"name": "Simons Family (Renaissance)", "tier": "individual", "category": "political_donor", "title": "Simons family — $25M to Democrats", "net_worth_estimate": 31_000_000_000, "influence_score": 0.80, "metadata": {"total_donated_2024": 25_000_000, "party": "Democrat", "recipients": ["Senate Majority PAC"], "industry": "hedge funds (RenTech)", "wants": "Science funding, education", "note": "Jim Simons died May 2024; family continues"}, "connections": [{"actor": "hf_simons", "type": "family"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_thiel": {"name": "Peter Thiel", "tier": "individual", "category": "political_donor", "title": "Thiel — $20M+ (tech libertarian)", "net_worth_estimate": 11_000_000_000, "influence_score": 0.86, "metadata": {"total_donated_2024": 20_000_000, "party": "Republican", "recipients": ["MAGA-aligned candidates"], "industry": "tech/VC (Palantir, Founders Fund)", "wants": "Tech deregulation, defense contracts (Palantir)"}, "connections": [{"actor": "pol_thiel", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest_ideological"},
    "donor_marcus": {"name": "Bernie Marcus", "tier": "individual", "category": "political_donor", "title": "Marcus (Home Depot) — $20M to GOP", "net_worth_estimate": 10_900_000_000, "influence_score": 0.76, "metadata": {"total_donated_2024": 20_000_000, "party": "Republican", "industry": "retail (Home Depot co-founder)", "wants": "Deregulation, low taxes, pro-Israel"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_sussman": {"name": "Donald Sussman", "tier": "individual", "category": "political_donor", "title": "Sussman — $18M to Democrats", "net_worth_estimate": 2_500_000_000, "influence_score": 0.72, "metadata": {"total_donated_2024": 18_000_000, "party": "Democrat", "industry": "hedge funds (Paloma Partners)", "wants": "Progressive policy"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_laufer": {"name": "Henry Laufer", "tier": "individual", "category": "political_donor", "title": "Laufer (RenTech) — $18M to Democrats", "net_worth_estimate": 3_500_000_000, "influence_score": 0.70, "metadata": {"total_donated_2024": 18_000_000, "party": "Democrat", "industry": "hedge funds (Renaissance Technologies)", "wants": "Science funding"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_blavatnik": {"name": "Len Blavatnik", "tier": "individual", "category": "political_donor", "title": "Blavatnik — $15M bipartisan (lean R)", "net_worth_estimate": 32_000_000_000, "influence_score": 0.78, "metadata": {"total_donated_2024": 15_000_000, "party": "Bipartisan (lean R)", "industry": "diversified (Access Industries)", "wants": "Business-friendly regulation"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},
    "donor_pritzker": {"name": "J.B. Pritzker", "tier": "individual", "category": "political_donor", "title": "Pritzker — IL Gov + $15M to Democrats", "net_worth_estimate": 3_500_000_000, "influence_score": 0.78, "metadata": {"total_donated_2024": 15_000_000, "party": "Democrat", "industry": "hospitality (Hyatt heir)", "wants": "Progressive policy, abortion rights, gun control"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_steyer": {"name": "Tom Steyer", "tier": "individual", "category": "political_donor", "title": "Steyer — $15M climate/Democrat donor", "net_worth_estimate": 1_600_000_000, "influence_score": 0.74, "metadata": {"total_donated_2024": 15_000_000, "party": "Democrat", "industry": "hedge funds (Farallon) / clean energy", "wants": "Climate policy, clean energy, wealth tax"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_mercer": {"name": "Rebekah Mercer", "tier": "individual", "category": "political_donor", "title": "Mercer — $12M conservative mega-donor", "net_worth_estimate": 1_000_000_000, "influence_score": 0.76, "metadata": {"total_donated_2024": 12_000_000, "party": "Republican", "industry": "finance (RenTech heir; backed Breitbart, Cambridge Analytica)", "wants": "Conservative media, anti-establishment politics"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_dell": {"name": "Michael Dell", "tier": "individual", "category": "political_donor", "title": "Dell — $12M bipartisan (lean R)", "net_worth_estimate": 100_000_000_000, "influence_score": 0.78, "metadata": {"total_donated_2024": 12_000_000, "party": "Republican-leaning", "industry": "tech (Dell Technologies)", "wants": "Tech-friendly policy, tax reform"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},
    "donor_saban": {"name": "Haim Saban", "tier": "individual", "category": "political_donor", "title": "Saban — $12M Democratic + pro-Israel donor", "net_worth_estimate": 3_100_000_000, "influence_score": 0.74, "metadata": {"total_donated_2024": 12_000_000, "party": "Democrat", "industry": "media/entertainment", "wants": "Pro-Israel policy"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_ross": {"name": "Stephen Ross", "tier": "individual", "category": "political_donor", "title": "Ross (Related Companies) — $10M real estate GOP donor", "net_worth_estimate": 12_500_000_000, "influence_score": 0.78, "metadata": {"total_donated_2024": 10_000_000, "party": "Republican", "industry": "real estate (Hudson Yards)", "wants": "Opportunity Zones, 1031 exchanges, favorable zoning"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},
    "donor_wynn": {"name": "Steve Wynn", "tier": "individual", "category": "political_donor", "title": "Wynn — $10M casino GOP donor", "net_worth_estimate": 3_500_000_000, "influence_score": 0.72, "metadata": {"total_donated_2024": 10_000_000, "party": "Republican", "industry": "casinos/hospitality", "wants": "Gaming regulation, tax policy"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},
    "donor_winklevoss": {"name": "Winklevoss twins", "tier": "individual", "category": "political_donor", "title": "Winklevoss — $8M crypto + Trump donors", "net_worth_estimate": 5_000_000_000, "influence_score": 0.72, "metadata": {"total_donated_2024": 8_000_000, "party": "Republican + crypto", "industry": "crypto (Gemini)", "wants": "Bitcoin-friendly regulation, oppose CBDC"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest_ideological"},
    "donor_katzenberg": {"name": "Jeffrey Katzenberg", "tier": "individual", "category": "political_donor", "title": "Katzenberg — Hollywood Democratic fundraiser", "influence_score": 0.72, "metadata": {"total_donated_2024": 8_000_000, "party": "Democrat", "industry": "entertainment (DreamWorks)", "wants": "IP protection, pro-Israel"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_lauder": {"name": "Ronald Lauder", "tier": "individual", "category": "political_donor", "title": "Lauder (Estee Lauder heir) — $8M GOP + pro-Israel", "net_worth_estimate": 4_600_000_000, "influence_score": 0.70, "metadata": {"total_donated_2024": 8_000_000, "party": "Republican", "industry": "cosmetics/art", "wants": "Pro-Israel policy, tax reform"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_cooperman": {"name": "Leon Cooperman", "tier": "individual", "category": "political_donor", "title": "Cooperman — $8M hedge fund GOP donor", "net_worth_estimate": 3_500_000_000, "influence_score": 0.70, "metadata": {"total_donated_2024": 8_000_000, "party": "Republican-leaning", "industry": "hedge funds (Omega)", "wants": "Anti-wealth-tax"}, "connections": [{"actor": "ind_cooperman", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},
    "donor_powell_jobs": {"name": "Laurene Powell Jobs", "tier": "individual", "category": "political_donor", "title": "Powell Jobs — $10M media/philanthropy Democrat", "net_worth_estimate": 16_000_000_000, "influence_score": 0.76, "metadata": {"total_donated_2024": 10_000_000, "party": "Democrat", "industry": "media/philanthropy (Emerson Collective, The Atlantic)", "wants": "Immigration reform, education, climate"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_diller": {"name": "Barry Diller", "tier": "individual", "category": "political_donor", "title": "Diller (IAC/Expedia) — bipartisan media donor", "net_worth_estimate": 4_400_000_000, "influence_score": 0.72, "metadata": {"total_donated_2024": 5_000_000, "party": "Democrat-leaning", "industry": "media/tech (IAC, Expedia)", "wants": "Open internet, media deregulation"}, "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "ideological"},
    "donor_tepper": {"name": "David Tepper", "tier": "individual", "category": "political_donor", "title": "Tepper (Appaloosa) — $10M bipartisan", "net_worth_estimate": 20_500_000_000, "influence_score": 0.76, "metadata": {"total_donated_2024": 10_000_000, "party": "Bipartisan", "industry": "hedge funds (Appaloosa)", "wants": "Low taxes"}, "connections": [{"actor": "hf_tepper", "type": "same_entity"}], "data_sources": ["opensecrets", "fec_filings"], "credibility": "hard_data", "motivation_model": "self_interest"},

    # ══════════════════════════════════════════════════════════════════════
    # TOP DEFENSE CONTRACTORS — military-industrial complex
    # Contract $, key programs, congressional supporters, revolving door
    # ══════════════════════════════════════════════════════════════════════
    "defense_lockheed": {"name": "Lockheed Martin", "tier": "institutional", "category": "defense_contractor", "title": "Largest defense contractor — $64.7B defense revenue (2024)", "influence_score": 0.96, "metadata": {"ticker": "LMT", "contract_value_2024": 64_700_000_000, "ceo": "Jim Taiclet", "key_programs": ["F-35 ($30B+)", "THAAD", "Aegis", "GPS III", "Orion"], "congressional_supporters": ["Rob Wittman (VA-1, HASC)", "Mike Rogers (AL-3, HASC Chair)", "Roger Wicker (MS, SASC Chair)"], "revolving_door": "44+ former Pentagon officials hired", "lobbying_spend_2024": 14_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_rtx": {"name": "RTX Corporation (Raytheon)", "tier": "institutional", "category": "defense_contractor", "title": "2nd largest — $80.7B total revenue", "influence_score": 0.94, "metadata": {"ticker": "RTX", "contract_value_2024": 24_100_000_000, "ceo": "Chris Calio", "key_programs": ["Patriot", "Stinger", "Pratt & Whitney", "AN/SPY-6"], "congressional_supporters": ["Joe Courtney (CT-2)", "Chris Murphy (CT)"], "revolving_door": "24+ officials; Lloyd Austin was on Raytheon board", "lobbying_spend_2024": 12_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_northrop": {"name": "Northrop Grumman", "tier": "institutional", "category": "defense_contractor", "title": "$35.2B defense revenue", "influence_score": 0.92, "metadata": {"ticker": "NOC", "contract_value_2024": 35_200_000_000, "ceo": "Kathy Warden", "key_programs": ["B-21 Raider", "Sentinel ICBM", "JWST", "Global Hawk"], "congressional_supporters": ["Ken Calvert (CA-41)", "Mike Turner (OH-10)"], "revolving_door": "24+ former Pentagon personnel", "lobbying_spend_2024": 11_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_general_dynamics": {"name": "General Dynamics", "tier": "institutional", "category": "defense_contractor", "title": "$33.7B defense revenue", "influence_score": 0.92, "metadata": {"ticker": "GD", "contract_value_2024": 33_700_000_000, "ceo": "Phebe Novakovic", "key_programs": ["Columbia-class subs", "Abrams tank", "Stryker", "GDIT"], "congressional_supporters": ["Jack Reed (RI)", "Tim Kaine (VA)"], "revolving_door": "8+ former Pentagon officials", "lobbying_spend_2024": 10_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_boeing": {"name": "Boeing Defense, Space & Security", "tier": "institutional", "category": "defense_contractor", "title": "Boeing Defense — $25B contracts", "influence_score": 0.92, "metadata": {"ticker": "BA", "contract_value_2024": 25_000_000_000, "ceo": "Kelly Ortberg", "key_programs": ["KC-46 tanker", "F/A-18", "Apache", "SLS rocket", "P-8 Poseidon"], "congressional_supporters": ["Maria Cantwell (WA)", "Patty Murray (WA)"], "revolving_door": "23+ former Pentagon officials", "lobbying_spend_2024": 11_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_l3harris": {"name": "L3Harris Technologies", "tier": "institutional", "category": "defense_contractor", "title": "6th largest — EW, ISR, comms", "influence_score": 0.86, "metadata": {"ticker": "LHX", "contract_value_2024": 20_000_000_000, "ceo": "Chris Kubasik", "key_programs": ["F/A-18 EW", "tactical radios", "ISR", "satellites"], "congressional_supporters": ["Bill Posey (FL-8)", "Marco Rubio (FL)"], "lobbying_spend_2024": 8_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_hii": {"name": "Huntington Ingalls Industries", "tier": "institutional", "category": "defense_contractor", "title": "Sole builder of US aircraft carriers — $11.4B", "influence_score": 0.86, "metadata": {"ticker": "HII", "contract_value_2024": 11_400_000_000, "ceo": "Chris Kastner", "key_programs": ["Ford-class carriers", "Virginia-class subs", "LPD ($9.6B)"], "congressional_supporters": ["Bobby Scott (VA-3)", "Roger Wicker (MS)"], "revolving_door": "Former Navy admirals on board", "lobbying_spend_2024": 6_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_bae": {"name": "BAE Systems (US)", "tier": "institutional", "category": "defense_contractor", "title": "8th largest — British-owned, US presence ($12B)", "influence_score": 0.84, "metadata": {"ticker": "BAESY", "contract_value_2024": 12_000_000_000, "ceo": "Tom Arseneault (BAE Inc.)", "key_programs": ["Bradley IFV", "amphibious combat", "EW", "ship repair"], "lobbying_spend_2024": 5_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_leidos": {"name": "Leidos", "tier": "institutional", "category": "defense_contractor", "title": "9th largest — defense IT, intel ($11.1B)", "influence_score": 0.82, "metadata": {"ticker": "LDOS", "contract_value_2024": 11_100_000_000, "ceo": "Tom Bell", "key_programs": ["defense IT", "intel community", "TSA"], "revolving_door": "Spun off from SAIC; deep IC ties", "lobbying_spend_2024": 5_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_booz_allen": {"name": "Booz Allen Hamilton", "tier": "institutional", "category": "defense_contractor", "title": "Intel community + defense consulting ($9B)", "influence_score": 0.82, "metadata": {"ticker": "BAH", "contract_value_2024": 9_000_000_000, "ceo": "Horacio Rozanski", "key_programs": ["NSA analytics", "Army AI/ML", "cyber ops"], "revolving_door": "Edward Snowden was contractor here. Deep NSA/CIA ties.", "lobbying_spend_2024": 4_000_000}, "data_sources": ["sec_edgar", "usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_saic": {"name": "SAIC", "tier": "institutional", "category": "defense_contractor", "title": "SAIC — defense IT ($7B)", "influence_score": 0.78, "metadata": {"ticker": "SAIC", "contract_value_2024": 7_000_000_000, "ceo": "Toni Townes-Whitley", "key_programs": ["defense IT", "space", "training"], "revolving_door": "Heavy intel revolving door"}, "data_sources": ["sec_edgar", "usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_caci": {"name": "CACI International", "tier": "institutional", "category": "defense_contractor", "title": "CACI — intel, cyber, C4ISR ($7B)", "influence_score": 0.76, "metadata": {"ticker": "CACI", "contract_value_2024": 7_000_000_000, "ceo": "John Mengucci", "key_programs": ["intel analytics", "cyber", "SIGINT"]}, "data_sources": ["sec_edgar", "usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_kbr": {"name": "KBR Inc.", "tier": "institutional", "category": "defense_contractor", "title": "KBR — services, logistics ($6.5B)", "influence_score": 0.76, "metadata": {"ticker": "KBR", "contract_value_2024": 6_500_000_000, "ceo": "Stuart Bradie", "key_programs": ["base ops", "logistics", "space"], "revolving_door": "Former Halliburton subsidiary"}, "data_sources": ["sec_edgar", "usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_ge_aerospace": {"name": "GE Aerospace (defense)", "tier": "institutional", "category": "defense_contractor", "title": "GE Aerospace — military jet engines ($6B)", "influence_score": 0.80, "metadata": {"ticker": "GE", "contract_value_2024": 6_000_000_000, "ceo": "Larry Culp", "key_programs": ["F110 (F-16)", "F414 (Super Hornet)", "T901 helicopter", "adaptive cycle"]}, "data_sources": ["sec_edgar", "usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_honeywell": {"name": "Honeywell Aerospace & Defense", "tier": "institutional", "category": "defense_contractor", "title": "Honeywell — avionics, engines ($5B)", "influence_score": 0.80, "metadata": {"ticker": "HON", "contract_value_2024": 5_000_000_000, "ceo": "Vimal Kapur", "key_programs": ["F-35 avionics", "helicopter engines", "smart munitions"]}, "data_sources": ["sec_edgar", "usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_textron": {"name": "Textron Inc.", "tier": "institutional", "category": "defense_contractor", "title": "Textron — Bell helicopters, Cessna ($5B)", "influence_score": 0.78, "metadata": {"ticker": "TXT", "contract_value_2024": 5_000_000_000, "ceo": "Scott Donnelly", "key_programs": ["V-280 Valor (FLRAA)", "Bell helicopters", "Shadow UAV"]}, "data_sources": ["sec_edgar", "usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_peraton": {"name": "Peraton (Veritas Capital)", "tier": "institutional", "category": "defense_contractor", "title": "Peraton — PE-owned intel/defense IT ($5B)", "influence_score": 0.74, "metadata": {"contract_value_2024": 5_000_000_000, "ceo": "Stu Shea", "key_programs": ["NRO satellite ops", "space command", "IC IT"], "note": "PE-owned (Veritas); less transparent"}, "data_sources": ["usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_general_atomics": {"name": "General Atomics", "tier": "institutional", "category": "defense_contractor", "title": "General Atomics — Predator/Reaper drones ($4.5B)", "influence_score": 0.82, "metadata": {"contract_value_2024": 4_500_000_000, "ceo": "Linden Blue", "key_programs": ["MQ-9 Reaper", "MQ-1C Gray Eagle", "EMALS", "EM railgun"], "note": "Private company — less transparency"}, "data_sources": ["usaspending_gov", "opensecrets"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_spacex": {"name": "SpaceX (defense)", "tier": "institutional", "category": "defense_contractor", "title": "SpaceX — rockets, Starlink for DoD ($4B)", "influence_score": 0.88, "metadata": {"contract_value_2024": 4_000_000_000, "president": "Gwynne Shotwell", "key_programs": ["Starlink/Starshield", "NRO launches", "Space Development Agency"], "revolving_door": "Musk is simultaneously top donor + defense contractor + DOGE head", "note": "Rose from #53 to #28. Massive conflict of interest."}, "connections": [{"actor": "ind_musk", "type": "controls"}], "data_sources": ["usaspending_gov", "nasa_contracts"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_palantir": {"name": "Palantir Technologies", "tier": "institutional", "category": "defense_contractor", "title": "Palantir — AI for defense + intel ($3B)", "influence_score": 0.84, "metadata": {"ticker": "PLTR", "contract_value_2024": 3_000_000_000, "ceo": "Alex Karp", "key_programs": ["Army TITAN", "Maven", "Gotham (IC)", "NATO AI"], "note": "Rose from #96 to top 50. Thiel co-founded."}, "connections": [{"actor": "pol_thiel", "type": "co_founder"}], "data_sources": ["sec_edgar", "usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_rolls_royce": {"name": "Rolls-Royce North America", "tier": "institutional", "category": "defense_contractor", "title": "Rolls-Royce — military engines, nuclear propulsion ($3B)", "influence_score": 0.74, "metadata": {"contract_value_2024": 3_000_000_000, "key_programs": ["B-52 re-engine (F130)", "sub nuclear reactors", "V-22 engines"]}, "data_sources": ["usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_anduril": {"name": "Anduril Industries", "tier": "institutional", "category": "defense_contractor", "title": "Anduril — AI defense startup ($1.5B, entered top 100)", "influence_score": 0.80, "metadata": {"contract_value_2024": 1_500_000_000, "ceo": "Palmer Luckey", "key_programs": ["Lattice AI", "Ghost drone", "Anvil counter-UAS", "autonomous subs"], "revolving_door": "Trae Stephens co-founder is ex-Pentagon", "note": "VC-backed $14B valuation; disrupting legacy contractors"}, "data_sources": ["usaspending_gov", "defense_news"], "credibility": "hard_data", "motivation_model": "profit_contracts"},
    "defense_shield_ai": {"name": "Shield AI", "tier": "institutional", "category": "defense_contractor", "title": "Shield AI — autonomous drone AI", "influence_score": 0.72, "metadata": {"contract_value_2024": 500_000_000, "ceo": "Brandon Tseng", "key_programs": ["Hivemind autonomous pilot", "V-BAT drone"], "note": "VC-backed; competitor to Anduril"}, "data_sources": ["usaspending_gov"], "credibility": "hard_data", "motivation_model": "profit_contracts"},

    # ══════════════════════════════════════════════════════════════════════
    # FEDERAL RESERVE REGIONAL BANK PRESIDENTS — all 12 districts
    # Voting rotation determines which hawks/doves shape rates.
    # ══════════════════════════════════════════════════════════════════════
    "fed_collins": {"name": "Susan Collins", "tier": "sovereign", "category": "central_bank", "title": "President, Fed Boston (District 1)", "influence_score": 0.78, "metadata": {"district": 1, "city": "Boston", "voting_2026": False, "lean": "neutral-to-dovish", "key_speeches": ["Labor market dynamics", "inflation expectations"], "term_start": 2022}, "data_sources": ["fed_speeches", "fomc_minutes"], "credibility": "hard_data", "motivation_model": "institutional_mandate"},
    # fed_williams already exists (NY, District 2) — permanent voter
    "fed_paulson": {"name": "Anna Paulson", "tier": "sovereign", "category": "central_bank", "title": "President, Fed Philadelphia (District 3)", "influence_score": 0.76, "metadata": {"district": 3, "city": "Philadelphia", "voting_2026": True, "lean": "neutral-to-dovish", "key_speeches": ["New president 2025 — establishing stance"], "term_start": 2025, "note": "Replaced Harker; from Chicago Fed research"}, "data_sources": ["fed_speeches", "fomc_minutes"], "credibility": "hard_data", "motivation_model": "institutional_mandate"},
    "fed_hammack": {"name": "Beth Hammack", "tier": "sovereign", "category": "central_bank", "title": "President, Fed Cleveland (District 4)", "influence_score": 0.80, "metadata": {"district": 4, "city": "Cleveland", "voting_2026": True, "lean": "hawkish", "key_speeches": ["Inflation persistence", "restrictive policy appropriate"], "term_start": 2024, "note": "Former Goldman Sachs CFO; hawkish"}, "data_sources": ["fed_speeches", "fomc_minutes"], "credibility": "hard_data", "motivation_model": "institutional_mandate"},
    "fed_barkin": {"name": "Thomas Barkin", "tier": "sovereign", "category": "central_bank", "title": "President, Fed Richmond (District 5)", "influence_score": 0.78, "metadata": {"district": 5, "city": "Richmond", "voting_2026": False, "lean": "neutral-to-hawkish", "key_speeches": ["Business survey approach", "inflation expectations"], "term_start": 2018}, "data_sources": ["fed_speeches", "fomc_minutes"], "credibility": "hard_data", "motivation_model": "institutional_mandate"},
    "fed_atlanta_tbd": {"name": "Atlanta Fed President (TBD — Bostic retired)", "tier": "sovereign", "category": "central_bank", "title": "President, Fed Atlanta (District 6) — VACANT", "influence_score": 0.72, "metadata": {"district": 6, "city": "Atlanta", "voting_2026": False, "lean": "TBD", "note": "Bostic retired; search underway. Cheryl Venable interim."}, "data_sources": ["fed_speeches"], "credibility": "hard_data", "motivation_model": "institutional_mandate"},
    # fed_goolsbee already exists (Chicago, District 7)
    # fed_musalem already exists (St. Louis, District 8)
    "fed_kashkari": {"name": "Neel Kashkari", "tier": "sovereign", "category": "central_bank", "title": "President, Fed Minneapolis (District 9)", "influence_score": 0.82, "metadata": {"district": 9, "city": "Minneapolis", "voting_2026": True, "lean": "hawkish (was dovish, turned)", "key_speeches": ["Keep rates higher until inflation beaten", "housing inflation persistence"], "term_start": 2016, "note": "Former TARP architect; dramatically shifted dove-to-hawk 2022-2025"}, "data_sources": ["fed_speeches", "fomc_minutes"], "credibility": "hard_data", "motivation_model": "institutional_mandate"},
    "fed_schmid": {"name": "Jeffrey Schmid", "tier": "sovereign", "category": "central_bank", "title": "President, Fed Kansas City (District 10)", "influence_score": 0.76, "metadata": {"district": 10, "city": "Kansas City", "voting_2026": False, "lean": "hawkish", "key_speeches": ["Voted to hold — inflation too high"], "term_start": 2023}, "data_sources": ["fed_speeches", "fomc_minutes"], "credibility": "hard_data", "motivation_model": "institutional_mandate"},
    "fed_logan": {"name": "Lorie Logan", "tier": "sovereign", "category": "central_bank", "title": "President, Fed Dallas (District 11)", "influence_score": 0.82, "metadata": {"district": 11, "city": "Dallas", "voting_2026": True, "lean": "hawkish", "key_speeches": ["Warns against premature cuts (Oct 2025)", "Hawkish pause (Feb 2026)", "Tariffs pose upside inflation risk"], "term_start": 2022, "note": "Former NY Fed markets desk head. Hawkish score 6.8."}, "data_sources": ["fed_speeches", "fomc_minutes"], "credibility": "hard_data", "motivation_model": "institutional_mandate"},
    # fed_daly already exists (San Francisco, District 12)

    # ══════════════════════════════════════════════════════════════════════
    # TOP REITs / REAL ESTATE DEVELOPERS
    # Control trillions in property; shape cities through zoning influence.
    # ══════════════════════════════════════════════════════════════════════
    "reit_prologis": {"name": "Prologis", "tier": "institutional", "category": "reit", "title": "World's largest logistics REIT (~$98B mkt cap)", "aum": 200_000_000_000, "influence_score": 0.86, "metadata": {"ticker": "PLD", "ceo": "Hamid Moghadam", "portfolio": "6,000+ buildings, 20 countries", "political_connections": "Industrial zoning, Opportunity Zones"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_american_tower": {"name": "American Tower", "tier": "institutional", "category": "reit", "title": "Wireless tower REIT (~$102B mkt cap)", "influence_score": 0.84, "metadata": {"ticker": "AMT", "ceo": "Steven Vondran", "portfolio": "224,000+ cell towers", "political_connections": "FCC spectrum, 5G lobbying"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_equinix": {"name": "Equinix", "tier": "institutional", "category": "reit", "title": "Largest data center REIT (~$85B mkt cap)", "influence_score": 0.84, "metadata": {"ticker": "EQIX", "ceo": "Adaire Fox-Martin", "portfolio": "260+ data centers, 32 countries", "political_connections": "Energy policy, data sovereignty"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_welltower": {"name": "Welltower", "tier": "institutional", "category": "reit", "title": "Healthcare/senior living REIT (~$98B mkt cap)", "influence_score": 0.82, "metadata": {"ticker": "WELL", "ceo": "Shankh Mitra", "portfolio": "2,500+ senior communities", "political_connections": "Healthcare policy, Medicare/Medicaid"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_simon": {"name": "Simon Property Group", "tier": "institutional", "category": "reit", "title": "Largest mall REIT ($69B mkt cap)", "influence_score": 0.82, "metadata": {"ticker": "SPG", "ceo": "David Simon", "portfolio": "245M sq ft retail", "political_connections": "Retail zoning, e-commerce tax"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_public_storage": {"name": "Public Storage", "tier": "institutional", "category": "reit", "title": "Largest self-storage REIT ($55B mkt cap)", "influence_score": 0.78, "metadata": {"ticker": "PSA", "ceo": "Joe Russell", "portfolio": "3,000+ facilities"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_digital_realty": {"name": "Digital Realty", "tier": "institutional", "category": "reit", "title": "Data center REIT ($50B mkt cap)", "influence_score": 0.80, "metadata": {"ticker": "DLR", "ceo": "Andy Power", "portfolio": "300+ data centers globally"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_realty_income": {"name": "Realty Income", "tier": "institutional", "category": "reit", "title": "'Monthly Dividend Company' ($48B mkt cap)", "influence_score": 0.78, "metadata": {"ticker": "O", "ceo": "Sumit Roy", "portfolio": "15,000+ properties"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_crown_castle": {"name": "Crown Castle", "tier": "institutional", "category": "reit", "title": "Wireless infrastructure REIT ($45B)", "influence_score": 0.78, "metadata": {"ticker": "CCI", "portfolio": "40,000+ towers, 115K miles fiber"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_blackstone_re": {"name": "Blackstone Real Estate", "tier": "institutional", "category": "developer", "title": "Largest commercial RE owner ($332B RE AUM)", "aum": 332_000_000_000, "influence_score": 0.92, "metadata": {"portfolio": "Largest commercial RE in world", "political_connections": "Schwarzman major Trump donor; Opportunity Zone beneficiary"}, "connections": [{"actor": "pc_blackstone", "type": "same_entity"}, {"actor": "pe_schwarzman", "type": "controls"}], "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_brookfield": {"name": "Brookfield Real Estate", "tier": "institutional", "category": "developer", "title": "Brookfield — $300B+ RE AUM", "aum": 300_000_000_000, "influence_score": 0.88, "metadata": {"ticker": "BAM/BN", "ceo": "Bruce Flatt", "political_connections": "Kushner 666 Fifth Ave bailout"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_related": {"name": "Related Companies", "tier": "institutional", "category": "developer", "title": "Hudson Yards developer — political heavyweight", "influence_score": 0.84, "metadata": {"founder": "Stephen Ross", "portfolio": "Hudson Yards ($25B), luxury residential", "political_connections": "Ross is Trump fundraiser; massive NYC zoning influence"}, "connections": [{"actor": "donor_ross", "type": "controls"}], "data_sources": ["nyc_disclosures"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_starwood": {"name": "Starwood Capital Group", "tier": "institutional", "category": "developer", "title": "Starwood — $100B AUM, hotels/multifamily", "aum": 100_000_000_000, "influence_score": 0.80, "metadata": {"founder": "Barry Sternlicht", "political_connections": "Sternlicht vocal on macro policy"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_vornado": {"name": "Vornado Realty Trust", "tier": "institutional", "category": "reit", "title": "NYC/DC office REIT ($8B)", "influence_score": 0.76, "metadata": {"ticker": "VNO", "ceo": "Steven Roth", "portfolio": "Penn District ($19B redevelopment)", "political_connections": "Roth was on Trump business council"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_kushner": {"name": "Kushner Companies", "tier": "institutional", "category": "developer", "title": "Kushner — political developer dynasty", "influence_score": 0.78, "metadata": {"founder": "Charles Kushner", "political_connections": "Jared Kushner was Trump senior advisor. Charles Kushner ambassador to France. 666 Fifth Ave Brookfield bailout."}, "data_sources": ["property_records"], "credibility": "hard_data", "motivation_model": "profit_political"},
    "reit_hines": {"name": "Hines", "tier": "institutional", "category": "developer", "title": "Hines — global RE, $94B portfolio", "influence_score": 0.78, "metadata": {"ceo": "Laura Hines-Pierce", "portfolio": "4,800 properties, 30 countries"}, "data_sources": ["property_records"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_tishman": {"name": "Tishman Speyer", "tier": "institutional", "category": "developer", "title": "Tishman Speyer — Rockefeller Center owner", "influence_score": 0.78, "metadata": {"ceo": "Rob Speyer", "portfolio": "Rockefeller Center, The Spiral"}, "data_sources": ["property_records"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_bxp": {"name": "BXP (Boston Properties)", "tier": "institutional", "category": "reit", "title": "Premier office REIT ($15B mkt cap)", "influence_score": 0.76, "metadata": {"ticker": "BXP", "ceo": "Owen Thomas", "portfolio": "55M+ sq ft office"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_iron_mountain": {"name": "Iron Mountain", "tier": "institutional", "category": "reit", "title": "Data storage REIT ($30B mkt cap)", "influence_score": 0.74, "metadata": {"ticker": "IRM", "portfolio": "1,400+ facilities"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_invitation_homes": {"name": "Invitation Homes", "tier": "institutional", "category": "reit", "title": "Largest single-family rental REIT ($20B)", "influence_score": 0.76, "metadata": {"ticker": "INVH", "ceo": "Dallas Tanner", "portfolio": "80,000+ homes", "note": "Poster child for institutional ownership of single-family homes controversy"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_ventas": {"name": "Ventas", "tier": "institutional", "category": "reit", "title": "Healthcare REIT ($27B mkt cap)", "influence_score": 0.74, "metadata": {"ticker": "VTR", "ceo": "Debra Cafaro", "portfolio": "1,200+ healthcare properties"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_avalonbay": {"name": "AvalonBay Communities", "tier": "institutional", "category": "reit", "title": "Premium apartment REIT ($32B)", "influence_score": 0.74, "metadata": {"ticker": "AVB", "portfolio": "300+ communities"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "reit_equity_residential": {"name": "Equity Residential", "tier": "institutional", "category": "reit", "title": "Sam Zell-founded apartment REIT ($28B)", "influence_score": 0.74, "metadata": {"ticker": "EQR", "portfolio": "300+ properties"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},

    # ══════════════════════════════════════════════════════════════════════
    # MAJOR MEDIA OWNERS — who controls what Americans see
    # Ownership = narrative control. The information gatekeepers.
    # ══════════════════════════════════════════════════════════════════════
    "media_murdoch": {"name": "Rupert Murdoch / Murdoch Family", "tier": "individual", "category": "media_owner", "title": "Fox News, WSJ, NY Post, Sky News Australia", "net_worth_estimate": 20_000_000_000, "influence_score": 0.95, "metadata": {"outlets": ["Fox News", "Fox Business", "WSJ", "NY Post", "HarperCollins"], "lean": "Right/conservative", "reach": "Fox #1 cable news; WSJ #1 business paper", "succession": "Lachlan now controls; family trust dispute ongoing"}, "connections": [{"actor": "dynasty_murdoch", "type": "same_entity"}], "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "ideological_profit"},
    "media_sulzberger": {"name": "A.G. Sulzberger / Ochs-Sulzberger Family", "tier": "individual", "category": "media_owner", "title": "NYT Publisher — owned since 1896", "influence_score": 0.90, "metadata": {"outlets": ["New York Times", "The Athletic", "Wirecutter"], "lean": "Center-left", "reach": "5.5B site visits/year — largest news viewership share", "editor_in_chief": "Joseph Kahn"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "institutional_prestige"},
    "media_bezos_wapo": {"name": "Jeff Bezos (Washington Post)", "tier": "individual", "category": "media_owner", "title": "Bought WaPo for $250M in 2013", "influence_score": 0.86, "metadata": {"outlets": ["Washington Post"], "lean": "Center-left (shifting)", "editor": "Matt Murray", "note": "Blocked 2024 endorsement — 250K subscribers cancelled"}, "connections": [{"actor": "ind_bezos", "type": "same_entity"}], "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "influence_prestige"},
    "media_bloomberg_lp": {"name": "Bloomberg LP / Bloomberg Media", "tier": "institutional", "category": "media_owner", "title": "Bloomberg Terminal + News — Wall Street's default", "influence_score": 0.92, "metadata": {"outlets": ["Bloomberg News", "Bloomberg TV", "Businessweek", "Bloomberg Terminal"], "lean": "Center / pro-business", "editor": "John Micklethwait", "reach": "350K+ terminal subscribers", "note": "Terminal creates information asymmetry"}, "connections": [{"actor": "donor_bloomberg", "type": "controls"}], "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "profit_influence"},
    "media_soon_shiong": {"name": "Patrick Soon-Shiong (LA Times)", "tier": "individual", "category": "media_owner", "title": "Bought LA Times for $500M (2018)", "net_worth_estimate": 7_500_000_000, "influence_score": 0.74, "metadata": {"outlets": ["Los Angeles Times", "San Diego Union-Tribune"], "lean": "Moderate/centrist", "note": "Blocked 2024 endorsement like Bezos/WaPo. Biotech billionaire."}, "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "influence_prestige"},
    "media_nexstar": {"name": "Nexstar Media Group", "tier": "institutional", "category": "media_owner", "title": "Largest local TV broadcaster — 200+ stations", "influence_score": 0.84, "metadata": {"ticker": "NXST", "ceo": "Perry Sook", "outlets": ["200+ TV stations (post-TEGNA)", "NewsNation", "The Hill"], "reach": "80%+ of US TV households after $6.2B TEGNA acquisition (approved March 2026)"}, "data_sources": ["sec_edgar", "fcc_filings"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_sinclair": {"name": "Sinclair Broadcast Group (Smith Family)", "tier": "institutional", "category": "media_owner", "title": "179 local TV stations — conservative lean", "influence_score": 0.82, "metadata": {"ticker": "SBGI", "controller": "David Smith (exec chair)", "outlets": ["179 local TV affiliates"], "lean": "Right/conservative", "reach": "40% of US households", "note": "Mandates must-run conservative segments; shrinks local political coverage; +6.4% ad time"}, "data_sources": ["sec_edgar", "fcc_filings"], "credibility": "hard_data", "motivation_model": "ideological_profit"},
    "media_gray_tv": {"name": "Gray Television", "tier": "institutional", "category": "media_owner", "title": "~180 TV stations in 110+ markets", "influence_score": 0.78, "metadata": {"ticker": "GTN", "ceo": "Hilton Howell Jr.", "outlets": ["180 TV stations"], "note": "Big Three local TV with Nexstar and Sinclair"}, "data_sources": ["sec_edgar", "fcc_filings"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_comcast": {"name": "Comcast / NBCUniversal (Roberts Family)", "tier": "institutional", "category": "media_owner", "title": "Largest media conglomerate by revenue", "influence_score": 0.90, "metadata": {"ticker": "CMCSA", "controller": "Brian Roberts", "outlets": ["NBC", "MSNBC", "CNBC", "Peacock", "Universal", "Xfinity"], "lean": "MSNBC left; CNBC pro-business", "note": "Vertical integration: content + distribution. Spun off cable nets 2025."}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_disney": {"name": "Walt Disney Company / Bob Iger", "tier": "institutional", "category": "media_owner", "title": "ABC, ESPN, Hulu, Disney+, FX", "influence_score": 0.88, "metadata": {"ticker": "DIS", "ceo": "Bob Iger", "outlets": ["ABC News", "ESPN", "FX", "Hulu", "Disney+"], "note": "DeSantis vs Disney war over FL special district"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_wbd": {"name": "Warner Bros. Discovery / David Zaslav", "tier": "institutional", "category": "media_owner", "title": "CNN, HBO, Max, Warner Bros", "influence_score": 0.82, "metadata": {"ticker": "WBD", "ceo": "David Zaslav", "outlets": ["CNN", "HBO", "Max", "Discovery"], "note": "Zaslav shifted CNN rightward; major cost-cutting"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_paramount": {"name": "Paramount / Skydance (Ellison)", "tier": "institutional", "category": "media_owner", "title": "CBS, Paramount+, MTV (acquired by Skydance)", "influence_score": 0.78, "metadata": {"new_owner": "David Ellison (son of Larry Ellison)", "outlets": ["CBS", "CBS News", "Paramount+", "MTV", "Nickelodeon"], "note": "Control shifted from Redstone to Ellison family"}, "connections": [{"actor": "ind_ellison", "type": "family"}], "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_news_corp": {"name": "News Corp (Murdoch print/digital)", "tier": "institutional", "category": "media_owner", "title": "WSJ, Barron's, MarketWatch, NY Post, Dow Jones", "influence_score": 0.84, "metadata": {"ticker": "NWSA", "ceo": "Robert Thomson", "outlets": ["WSJ", "Barron's", "MarketWatch", "NY Post", "Dow Jones", "Realtor.com"], "lean": "Center-right"}, "connections": [{"actor": "media_murdoch", "type": "controls"}], "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "ideological_profit"},
    "media_gannett": {"name": "Gannett / USA Today", "tier": "institutional", "category": "media_owner", "title": "Largest newspaper chain — 250+ papers", "influence_score": 0.76, "metadata": {"ticker": "GCI", "ceo": "Mike Reed", "outlets": ["USA Today", "250+ local papers"], "note": "Massive layoffs; local news deserts expanding"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_survival"},
    "media_alden": {"name": "Alden Global Capital (Tribune/MediaNews)", "tier": "institutional", "category": "media_owner", "title": "Hedge fund that gutted local newspapers", "influence_score": 0.76, "metadata": {"founder": "Randall Smith", "outlets": ["Chicago Tribune", "NY Daily News", "Hartford Courant", "200+ papers via MediaNews Group"], "note": "'Grim Reaper of newspapers' — buys, guts staff, extracts cash"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_extraction"},
    "media_iheart": {"name": "iHeartMedia", "tier": "institutional", "category": "media_owner", "title": "Largest US radio company — 850+ stations", "influence_score": 0.78, "metadata": {"ticker": "IHRT", "ceo": "Bob Pittman", "outlets": ["850+ radio stations", "iHeartRadio"], "reach": "90% of US adults monthly", "note": "Talk radio skews right; music apolitical"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_cox": {"name": "Cox Media Group (Apollo-owned)", "tier": "institutional", "category": "media_owner", "title": "12 TV + 50 radio stations (PE-owned)", "influence_score": 0.72, "metadata": {"owner": "Apollo Global Management", "outlets": ["WSB Atlanta", "12 TV stations", "50 radio stations"], "note": "Apollo considering sale; Nexstar and Gray interested"}, "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_powell_jobs_atlantic": {"name": "Laurene Powell Jobs (The Atlantic)", "tier": "individual", "category": "media_owner", "title": "Owns The Atlantic via Emerson Collective", "influence_score": 0.72, "metadata": {"outlets": ["The Atlantic"], "lean": "Center-left", "editor": "Jeffrey Goldberg"}, "connections": [{"actor": "donor_powell_jobs", "type": "same_entity"}], "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "influence_prestige"},
    "media_benioff": {"name": "Marc Benioff (TIME)", "tier": "individual", "category": "media_owner", "title": "Bought TIME for $190M (2018)", "net_worth_estimate": 7_500_000_000, "influence_score": 0.72, "metadata": {"outlets": ["TIME magazine"], "lean": "Center-left", "note": "Salesforce CEO"}, "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "influence_prestige"},
    "media_dotdash_meredith": {"name": "Dotdash Meredith (IAC/Diller)", "tier": "institutional", "category": "media_owner", "title": "People, InStyle, Investopedia (IAC subsidiary)", "influence_score": 0.70, "metadata": {"parent": "IAC (Barry Diller)", "outlets": ["People", "InStyle", "Investopedia", "Better Homes & Gardens"], "reach": "300M+ monthly visitors"}, "data_sources": ["sec_edgar"], "credibility": "hard_data", "motivation_model": "profit_growth"},
    "media_vox": {"name": "Vox Media", "tier": "institutional", "category": "media_owner", "title": "NY Mag, The Verge, Vox — progressive digital", "influence_score": 0.72, "metadata": {"ceo": "Jim Bankoff", "outlets": ["New York Magazine", "The Verge", "Vox", "Vulture", "Eater"], "lean": "Left/progressive"}, "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "ideological_profit"},
    "media_daily_wire": {"name": "The Daily Wire (Shapiro/Boreing)", "tier": "institutional", "category": "media_owner", "title": "Most-engaged conservative digital media", "influence_score": 0.76, "metadata": {"founders": ["Ben Shapiro", "Jeremy Boreing"], "outlets": ["Daily Wire", "DailyWire+", "Bentkey"], "lean": "Right/conservative", "reach": "Top Facebook engagement; 1M+ subscribers"}, "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "ideological_profit"},
    "media_substack": {"name": "Substack", "tier": "institutional", "category": "media_owner", "title": "Newsletter platform — 35M+ subscriptions", "influence_score": 0.74, "metadata": {"founders": ["Chris Best", "Hamish McKenzie"], "reach": "35M+ subscriptions", "note": "Shifted power from editors to individual journalists"}, "data_sources": ["media_reports"], "credibility": "hard_data", "motivation_model": "platform_growth"},

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
    #   - regulated_by: entity is regulated by government/party
    #   - controlled_by: entity is controlled by state/party
    #   - family: family relationship
    #   - LP_relationship: limited partner in fund
    #   - state_linkage: connected through state ownership
    #   - strategic_partner: bilateral strategic relationship
    #   - appoints: political appointment power
    #   - institutional_peer: same category institutional peer
    #   - lp_investor: pension/endowment LP in fund
    #   - founder: individual founded the entity
}

# Confirm count at module-load time for development
_ACTOR_COUNT = len(_KNOWN_ACTORS)
assert _ACTOR_COUNT >= 475, (
    f"Expected >= 475 known actors (deep US + global coverage), got {_ACTOR_COUNT}. Add more seed data."
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
                SELECT ss.source_id, ss.ticker, ss.signal_type,
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
                SELECT source_id, ticker, signal_type,
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
                SELECT source_id, ticker, signal_type,
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
                SELECT ticker, signal_type, signal_date,
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
                SELECT DISTINCT ticker, signal_type, signal_date
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
                SELECT source_type, source_id, ticker, signal_type,
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
                SELECT source_id, ticker, signal_type, signal_date,
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
                    SELECT source_id, ticker, signal_type, signal_date
                    FROM signal_sources
                    WHERE source_type = 'insider'
                      AND signal_date >= :cutoff
                ),
                fund_trades AS (
                    SELECT source_id, ticker, signal_type, signal_date
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
                  AND ss.signal_type = 'SELL'
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
                SELECT source_id, ticker, signal_type, signal_date
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
                SELECT source_type, source_id, signal_type,
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
