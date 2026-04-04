"""
GRID Intelligence — Company Analyzer Pipeline.

Systematically researches every major company and builds influence profiles
by aggregating data from ALL intelligence modules: government contracts,
congressional trading, insider filings, lobbying, campaign finance,
legislation, export controls, and the actor network.

Key entry points:
    analyze_company              — full profile for a single ticker
    run_analysis_queue           — batch-process next N unanalyzed companies
    get_all_profiles             — all analyzed companies, sorted by suspicion
    find_cross_company_patterns  — detect cross-company influence patterns
    generate_sector_influence_report — LLM narrative for a sector

Data table: company_profiles
Wired into LLM task queue as P3 background work.
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
# ANALYSIS QUEUE — NASDAQ 100 + key private companies
# ══════════════════════════════════════════════════════════════════════════

ANALYSIS_QUEUE: list[str] = [
    # NASDAQ 100 (top by market cap)
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "COST", "NFLX",
    "AMD", "ADBE", "QCOM", "TXN", "INTC", "AMAT", "LRCX", "KLAC", "MRVL", "SNPS",
    "CDNS", "ASML", "MU", "ON", "MCHP", "ADI", "NXPI", "FTNT", "CRWD", "PANW",
    "CSCO", "CMCSA", "PEP", "TMUS", "AMGN", "HON", "ISRG", "REGN", "VRTX", "GILD",
    "BKNG", "ADP", "SBUX", "MDLZ", "PYPL", "LULU", "MNST", "KDP", "ORLY", "AZN",
    "ABNB", "DASH", "TEAM", "DDOG", "ZS", "TTD", "WDAY", "ROST", "EXC", "CEG",
    "BKR", "FANG", "GEHC", "CTAS", "MAR", "IDXX", "CPRT", "PAYX", "FAST", "PCAR",
    "ODFL", "VRSK", "EA", "CDW", "BIIB", "ILMN", "MRNA", "DXCM", "ENPH", "ANSS",
    "MELI", "WBD", "SIRI", "LCID", "RIVN", "ARM", "PLTR", "COIN", "SMCI", "MSTR",
    # Key private companies that influence markets
    "SPACEX", "OPENAI", "ANDURIL", "PALANTIR_PRIVATE", "STRIPE", "DATABRICKS",
    "CANVA", "SHEIN", "BYTEDANCE", "ANTHROPIC",
]

# Ticker -> human-readable name (for display and LLM prompts)
_TICKER_NAMES: dict[str, str] = {
    "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Alphabet/Google",
    "AMZN": "Amazon", "NVDA": "NVIDIA", "META": "Meta Platforms",
    "TSLA": "Tesla", "AVGO": "Broadcom", "COST": "Costco", "NFLX": "Netflix",
    "AMD": "AMD", "ADBE": "Adobe", "QCOM": "Qualcomm", "TXN": "Texas Instruments",
    "INTC": "Intel", "AMAT": "Applied Materials", "LRCX": "Lam Research",
    "KLAC": "KLA Corporation", "MRVL": "Marvell Technology", "SNPS": "Synopsys",
    "CDNS": "Cadence Design", "ASML": "ASML", "MU": "Micron Technology",
    "ON": "ON Semiconductor", "MCHP": "Microchip Technology", "ADI": "Analog Devices",
    "NXPI": "NXP Semiconductors", "FTNT": "Fortinet", "CRWD": "CrowdStrike",
    "PANW": "Palo Alto Networks", "CSCO": "Cisco Systems", "CMCSA": "Comcast",
    "PEP": "PepsiCo", "TMUS": "T-Mobile", "AMGN": "Amgen", "HON": "Honeywell",
    "ISRG": "Intuitive Surgical", "REGN": "Regeneron", "VRTX": "Vertex Pharma",
    "GILD": "Gilead Sciences", "BKNG": "Booking Holdings", "ADP": "ADP",
    "SBUX": "Starbucks", "MDLZ": "Mondelez", "PYPL": "PayPal", "LULU": "Lululemon",
    "MNST": "Monster Beverage", "KDP": "Keurig Dr Pepper", "ORLY": "O'Reilly Auto",
    "AZN": "AstraZeneca", "ABNB": "Airbnb", "DASH": "DoorDash", "TEAM": "Atlassian",
    "DDOG": "Datadog", "ZS": "Zscaler", "TTD": "The Trade Desk",
    "WDAY": "Workday", "ROST": "Ross Stores", "EXC": "Exelon", "CEG": "Constellation Energy",
    "BKR": "Baker Hughes", "FANG": "Diamondback Energy", "GEHC": "GE HealthCare",
    "CTAS": "Cintas", "MAR": "Marriott", "IDXX": "IDEXX Laboratories",
    "CPRT": "Copart", "PAYX": "Paychex", "FAST": "Fastenal", "PCAR": "PACCAR",
    "ODFL": "Old Dominion Freight", "VRSK": "Verisk Analytics", "EA": "Electronic Arts",
    "CDW": "CDW Corporation", "BIIB": "Biogen", "ILMN": "Illumina",
    "MRNA": "Moderna", "DXCM": "DexCom", "ENPH": "Enphase Energy",
    "ANSS": "ANSYS", "MELI": "MercadoLibre", "WBD": "Warner Bros. Discovery",
    "SIRI": "SiriusXM", "LCID": "Lucid Motors", "RIVN": "Rivian",
    "ARM": "Arm Holdings", "PLTR": "Palantir Technologies", "COIN": "Coinbase",
    "SMCI": "Super Micro Computer", "MSTR": "MicroStrategy",
    "SPACEX": "SpaceX", "OPENAI": "OpenAI", "ANDURIL": "Anduril Industries",
    "PALANTIR_PRIVATE": "Palantir (Private)", "STRIPE": "Stripe",
    "DATABRICKS": "Databricks", "CANVA": "Canva", "SHEIN": "Shein",
    "BYTEDANCE": "ByteDance/TikTok", "ANTHROPIC": "Anthropic",
}

# Sector classification for aggregation
_TICKER_SECTORS: dict[str, str] = {
    **{t: "Technology" for t in [
        "AAPL", "MSFT", "GOOGL", "META", "CSCO", "ADBE", "PYPL", "TEAM",
        "DDOG", "ZS", "TTD", "WDAY", "CDW", "COIN", "SMCI", "MSTR",
        "OPENAI", "STRIPE", "DATABRICKS", "CANVA", "ANTHROPIC",
    ]},
    **{t: "Semiconductors" for t in [
        "NVDA", "AVGO", "AMD", "QCOM", "TXN", "INTC", "AMAT", "LRCX",
        "KLAC", "MRVL", "SNPS", "CDNS", "ASML", "MU", "ON", "MCHP",
        "ADI", "NXPI", "ARM",
    ]},
    **{t: "Cybersecurity" for t in ["FTNT", "CRWD", "PANW"]},
    **{t: "Consumer" for t in [
        "AMZN", "COST", "NFLX", "PEP", "SBUX", "MDLZ", "LULU", "MNST",
        "KDP", "ORLY", "ROST", "FAST", "SHEIN", "BYTEDANCE",
    ]},
    **{t: "Healthcare" for t in [
        "AMGN", "ISRG", "REGN", "VRTX", "GILD", "AZN", "BIIB", "ILMN",
        "MRNA", "DXCM", "GEHC", "IDXX",
    ]},
    **{t: "Defense/Aerospace" for t in ["ANDURIL", "PALANTIR_PRIVATE", "PLTR", "SPACEX"]},
    **{t: "Telecom" for t in ["TMUS", "CMCSA", "SIRI"]},
    **{t: "Travel" for t in ["BKNG", "ABNB", "MAR"]},
    **{t: "Services" for t in ["ADP", "CTAS", "PAYX", "CPRT", "VRSK"]},
    **{t: "Automotive/EV" for t in ["TSLA", "LCID", "RIVN"]},
    **{t: "Energy" for t in ["EXC", "CEG", "BKR", "FANG", "ENPH"]},
    **{t: "Logistics" for t in ["ODFL", "PCAR", "DASH"]},
    **{t: "Industrial" for t in ["HON", "ANSS"]},
    **{t: "Entertainment" for t in ["EA", "WBD"]},
    "MELI": "E-Commerce/LatAm",
}


# ══════════════════════════════════════════════════════════════════════════
# DATA CLASS
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class CompanyProfile:
    """Full influence profile for a single company."""

    ticker: str
    name: str
    sector: str
    market_cap: float

    # Government contracts
    gov_contracts_total: float
    gov_contracts_count: int
    top_agencies: list[str]

    # Congressional holdings & overlap
    congress_holders: list[dict]  # [{member, shares_est, committee}]
    committee_overlap_count: int  # members on relevant committees who hold

    # Insider activity (90d window)
    insider_net_direction: str  # net_buying / net_selling / neutral
    insider_total_value_90d: float
    cluster_signals: int

    # Lobbying
    lobbying_spend_annual: float
    lobbying_trend: str  # increasing / decreasing / stable
    top_issues: list[str]

    # Influence network
    influence_loops: int
    suspicion_score: float
    hypocrisy_flags: int

    # Export/regulatory
    export_control_risk: str
    regulatory_actions: int

    # LLM narrative
    analysis_narrative: str
    last_analyzed: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ══════════════════════════════════════════════════════════════════════════
# SCHEMA
# ══════════════════════════════════════════════════════════════════════════

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS company_profiles (
    ticker              TEXT PRIMARY KEY,
    name                TEXT,
    sector              TEXT,
    profile             JSONB NOT NULL,
    narrative           TEXT,
    suspicion_score     NUMERIC DEFAULT 0,
    last_analyzed       TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_company_profiles_sector ON company_profiles (sector);",
    "CREATE INDEX IF NOT EXISTS idx_company_profiles_suspicion ON company_profiles (suspicion_score DESC);",
]


def ensure_table(engine: Engine) -> None:
    """Create the company_profiles table and indexes if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE_SQL))
        for idx_sql in _CREATE_INDEX_SQL:
            conn.execute(text(idx_sql))
    log.info("company_profiles table ensured")


# ══════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS — gather data from all intelligence modules
# ══════════════════════════════════════════════════════════════════════════


def _gather_gov_contracts(engine: Engine, ticker: str) -> dict[str, Any]:
    """Fetch government contract data for a ticker."""
    try:
        from intelligence.gov_intel import get_contracts_for_ticker
        contracts = get_contracts_for_ticker(engine, ticker)
        total = sum(c.amount for c in contracts)
        agencies = list({c.awarding_agency for c in contracts if c.awarding_agency})
        return {
            "total": total,
            "count": len(contracts),
            "top_agencies": agencies[:10],
        }
    except Exception as exc:
        log.debug("Gov contracts for {t} failed: {e}", t=ticker, e=str(exc))
        return {"total": 0.0, "count": 0, "top_agencies": []}


def _gather_legislative(engine: Engine, ticker: str) -> dict[str, Any]:
    """Fetch active legislation affecting a ticker."""
    try:
        from intelligence.legislative_intel import get_bills_affecting_ticker
        bills = get_bills_affecting_ticker(engine, ticker)
        return {
            "count": len(bills),
            "bills": bills[:10],
        }
    except Exception as exc:
        log.debug("Legislative intel for {t} failed: {e}", t=ticker, e=str(exc))
        return {"count": 0, "bills": []}


def _gather_influence(engine: Engine, ticker: str) -> dict[str, Any]:
    """Fetch influence network data for a ticker."""
    try:
        from intelligence.influence_network import get_influence_for_ticker
        data = get_influence_for_ticker(engine, ticker)
        return {
            "lobbying_total": data.get("lobbying", {}).get("total_spend", 0),
            "pac_total": data.get("pac_contributions", {}).get("total", 0),
            "contracts_total": data.get("contracts", {}).get("total_value", 0),
            "member_trades": data.get("member_trades", []),
            "circular_flow": data.get("circular_flow", {}),
            "hypocrisy_flags": data.get("circular_flow", {}).get("hypocrisy_flags", []),
            "lobbying_filings": data.get("lobbying", {}).get("filings", []),
        }
    except Exception as exc:
        log.debug("Influence network for {t} failed: {e}", t=ticker, e=str(exc))
        return {
            "lobbying_total": 0, "pac_total": 0, "contracts_total": 0,
            "member_trades": [], "circular_flow": {},
            "hypocrisy_flags": [], "lobbying_filings": [],
        }


def _gather_insider_edge(engine: Engine, ticker: str) -> dict[str, Any]:
    """Fetch insider/congressional trading edge."""
    try:
        from intelligence.trust_scorer import get_insider_edge
        edge = get_insider_edge(engine, ticker)
        if not edge:
            return {"direction": "neutral", "total_value": 0.0, "clusters": 0, "holders": []}

        # Aggregate congressional holders
        cong = edge.get("congressional", [])
        holders = []
        buy_value = 0.0
        sell_value = 0.0
        for trade in cong:
            val = trade if isinstance(trade, dict) else {}
            action = val.get("signal_type", "").upper()
            member = val.get("source_id", val.get("member", ""))
            committee = val.get("committee", "")
            amt_str = val.get("amount_range", val.get("amount", "0"))
            # Parse amount range — use midpoint estimate
            amt = _parse_amount_range(amt_str)
            holders.append({"member": member, "shares_est": amt, "committee": committee})
            if action == "BUY":
                buy_value += amt
            elif action == "SELL":
                sell_value += amt

        # Insider signals
        insider = edge.get("insider", [])
        insider_buy = 0.0
        insider_sell = 0.0
        cluster_count = 0
        for sig in insider:
            val = sig if isinstance(sig, dict) else {}
            action = val.get("signal_type", "").upper()
            amt = float(val.get("amount", 0) or 0)
            if action == "BUY":
                insider_buy += amt
            elif action == "SELL":
                insider_sell += amt
            if val.get("cluster"):
                cluster_count += 1

        net = (buy_value + insider_buy) - (sell_value + insider_sell)
        direction = "net_buying" if net > 0 else "net_selling" if net < 0 else "neutral"

        return {
            "direction": direction,
            "total_value": abs(net),
            "clusters": cluster_count,
            "holders": holders,
        }
    except Exception as exc:
        log.debug("Insider edge for {t} failed: {e}", t=ticker, e=str(exc))
        return {"direction": "neutral", "total_value": 0.0, "clusters": 0, "holders": []}


def _gather_lever_pullers(engine: Engine, ticker: str) -> dict[str, Any]:
    """Fetch lever puller context for a ticker."""
    try:
        from intelligence.lever_pullers import get_lever_context_for_ticker
        ctx = get_lever_context_for_ticker(engine, ticker)
        return ctx if isinstance(ctx, dict) else {}
    except Exception as exc:
        log.debug("Lever pullers for {t} failed: {e}", t=ticker, e=str(exc))
        return {}


def _gather_export_controls(engine: Engine, ticker: str) -> dict[str, Any]:
    """Fetch export control risk assessment."""
    try:
        from intelligence.export_intel import get_controls_for_ticker
        controls = get_controls_for_ticker(engine, ticker)
        if isinstance(controls, dict):
            return controls
        return {"risk_level": "NONE", "restrictions": 0}
    except Exception as exc:
        log.debug("Export controls for {t} failed: {e}", t=ticker, e=str(exc))
        return {"risk_level": "UNKNOWN", "restrictions": 0}


def _gather_actor_context(engine: Engine, ticker: str) -> dict[str, Any]:
    """Fetch actor network context for a ticker."""
    try:
        from intelligence.actor_network import get_actor_context_for_ticker
        ctx = get_actor_context_for_ticker(engine, ticker)
        return ctx if isinstance(ctx, dict) else {}
    except Exception as exc:
        log.debug("Actor context for {t} failed: {e}", t=ticker, e=str(exc))
        return {}


def _get_market_cap(engine: Engine, ticker: str) -> float:
    """Attempt to retrieve market cap from stored data."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date DESC LIMIT 1"
                ),
                {"pattern": f"YFINANCE:{ticker}:marketCap%"},
            ).fetchone()
            return float(row[0]) if row else 0.0
    except Exception:
        return 0.0


def _parse_amount_range(amt_str: Any) -> float:
    """Parse an amount range string like '$1,001 - $15,000' to midpoint."""
    if isinstance(amt_str, (int, float)):
        return float(amt_str)
    if not isinstance(amt_str, str):
        return 0.0
    # Strip $ and commas, try direct parse
    cleaned = amt_str.replace("$", "").replace(",", "").strip()
    if " - " in cleaned:
        parts = cleaned.split(" - ")
        try:
            low = float(parts[0].strip())
            high = float(parts[1].strip())
            return (low + high) / 2
        except (ValueError, IndexError):
            pass
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _compute_lobbying_trend(filings: list[dict]) -> str:
    """Determine if lobbying spend is increasing, decreasing, or stable."""
    if len(filings) < 2:
        return "stable"
    # Split into halves by date and compare totals
    sorted_filings = sorted(filings, key=lambda f: f.get("date", ""))
    mid = len(sorted_filings) // 2
    older_total = sum(float(f.get("amount", 0) or 0) for f in sorted_filings[:mid])
    newer_total = sum(float(f.get("amount", 0) or 0) for f in sorted_filings[mid:])
    if newer_total > older_total * 1.2:
        return "increasing"
    elif newer_total < older_total * 0.8:
        return "decreasing"
    return "stable"


def _extract_top_issues(filings: list[dict]) -> list[str]:
    """Extract unique lobbying issue codes from filings."""
    issues: dict[str, int] = defaultdict(int)
    for f in filings:
        for code in f.get("issue_codes", []):
            if isinstance(code, str) and code.strip():
                issues[code.strip()] += 1
    return [k for k, _ in sorted(issues.items(), key=lambda x: -x[1])][:10]


def _generate_narrative(engine: Engine, ticker: str, profile: CompanyProfile) -> str:
    """Generate an LLM narrative summarizing the company's influence profile."""
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.REASON)
        if client is None or not client.is_available:
            return _fallback_narrative(profile)
    except Exception:
        return _fallback_narrative(profile)

    prompt = (
        f"You are GRID Intelligence. Write a 2-3 paragraph analysis of "
        f"{profile.name} ({profile.ticker}) based on this influence data:\n\n"
        f"- Sector: {profile.sector}\n"
        f"- Government contracts: ${profile.gov_contracts_total:,.0f} "
        f"across {profile.gov_contracts_count} awards\n"
        f"- Top agencies: {', '.join(profile.top_agencies[:5]) or 'None'}\n"
        f"- Congressional holders: {len(profile.congress_holders)} members\n"
        f"- Committee overlap: {profile.committee_overlap_count} members on "
        f"relevant committees holding stock\n"
        f"- Insider activity (90d): {profile.insider_net_direction}, "
        f"${profile.insider_total_value_90d:,.0f}\n"
        f"- Cluster buy signals: {profile.cluster_signals}\n"
        f"- Lobbying spend: ${profile.lobbying_spend_annual:,.0f}/yr "
        f"({profile.lobbying_trend})\n"
        f"- Top lobbying issues: {', '.join(profile.top_issues[:5]) or 'None'}\n"
        f"- Influence loops detected: {profile.influence_loops}\n"
        f"- Suspicion score: {profile.suspicion_score:.3f}\n"
        f"- Hypocrisy flags: {profile.hypocrisy_flags}\n"
        f"- Export control risk: {profile.export_control_risk}\n"
        f"- Regulatory actions: {profile.regulatory_actions}\n\n"
        f"Focus on: Who is positioned around this company? What circular flows "
        f"of money exist? What is the regulatory/political risk? What does the "
        f"insider activity suggest? Be specific and data-driven."
    )

    try:
        result = client.chat(
            [
                {"role": "system", "content": (
                    "You are a financial intelligence analyst. Write concise, "
                    "data-driven analysis. No disclaimers or caveats — just the analysis."
                )},
                {"role": "user", "content": prompt},
            ],
            temperature=0.4,
            num_predict=800,
        )
        return result or _fallback_narrative(profile)
    except Exception:
        return _fallback_narrative(profile)


def _fallback_narrative(profile: CompanyProfile) -> str:
    """Generate a basic narrative without LLM."""
    parts = [f"{profile.name} ({profile.ticker}) — {profile.sector}."]

    if profile.gov_contracts_total > 0:
        parts.append(
            f"Government contracts: ${profile.gov_contracts_total:,.0f} "
            f"across {profile.gov_contracts_count} awards."
        )

    if profile.congress_holders:
        parts.append(
            f"{len(profile.congress_holders)} congressional members hold positions; "
            f"{profile.committee_overlap_count} sit on relevant oversight committees."
        )

    if profile.lobbying_spend_annual > 0:
        parts.append(
            f"Annual lobbying: ${profile.lobbying_spend_annual:,.0f} "
            f"({profile.lobbying_trend})."
        )

    if profile.suspicion_score > 0.3:
        parts.append(
            f"Suspicion score {profile.suspicion_score:.3f} with "
            f"{profile.influence_loops} influence loops and "
            f"{profile.hypocrisy_flags} hypocrisy flags."
        )

    parts.append(
        f"Insider direction: {profile.insider_net_direction} "
        f"(${profile.insider_total_value_90d:,.0f} over 90d)."
    )

    return " ".join(parts)


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════


def analyze_company(engine: Engine, ticker: str) -> CompanyProfile:
    """Run full influence analysis on a single company.

    Queries ALL intelligence modules, aggregates into a CompanyProfile,
    generates an LLM narrative, and stores the result in the database.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.

    Returns:
        Populated CompanyProfile dataclass.
    """
    ensure_table(engine)
    ticker = ticker.strip().upper()
    name = _TICKER_NAMES.get(ticker, ticker)
    sector = _TICKER_SECTORS.get(ticker, "Other")

    log.info("Analyzing company: {t} ({n})", t=ticker, n=name)

    # Gather from all intelligence modules
    gov = _gather_gov_contracts(engine, ticker)
    _gather_legislative(engine, ticker)  # side data, enriches context
    influence = _gather_influence(engine, ticker)
    insider = _gather_insider_edge(engine, ticker)
    _gather_lever_pullers(engine, ticker)  # enriches actor context
    export = _gather_export_controls(engine, ticker)
    _gather_actor_context(engine, ticker)  # enriches network

    market_cap = _get_market_cap(engine, ticker)

    # Compute lobbying metrics
    lobbying_filings = influence.get("lobbying_filings", [])
    lobbying_trend = _compute_lobbying_trend(lobbying_filings)
    top_issues = _extract_top_issues(lobbying_filings)

    # Compute influence metrics
    circular = influence.get("circular_flow", {})
    hypocrisy_count = len(influence.get("hypocrisy_flags", []))
    loops_detected = 1 if circular.get("detected") else 0

    # Suspicion score from influence data
    total_money = (
        influence.get("lobbying_total", 0)
        + influence.get("pac_total", 0)
        + influence.get("contracts_total", 0)
    )
    suspicion = _compute_company_suspicion(
        gov_total=gov["total"],
        lobbying_total=influence.get("lobbying_total", 0),
        pac_total=influence.get("pac_total", 0),
        circular_detected=circular.get("detected", False),
        hypocrisy_count=hypocrisy_count,
        committee_overlap=insider.get("holders", []),
        insider_direction=insider.get("direction", "neutral"),
        total_money=total_money,
    )

    # Congressional holders with committee overlap
    holders = insider.get("holders", [])
    committee_overlap = sum(
        1 for h in holders if h.get("committee")
    )

    # Export control risk
    export_risk = export.get("risk_level", "UNKNOWN")
    regulatory_count = export.get("restrictions", 0)

    now = datetime.now(timezone.utc).isoformat()

    profile = CompanyProfile(
        ticker=ticker,
        name=name,
        sector=sector,
        market_cap=market_cap,
        gov_contracts_total=gov["total"],
        gov_contracts_count=gov["count"],
        top_agencies=gov["top_agencies"],
        congress_holders=holders,
        committee_overlap_count=committee_overlap,
        insider_net_direction=insider.get("direction", "neutral"),
        insider_total_value_90d=insider.get("total_value", 0.0),
        cluster_signals=insider.get("clusters", 0),
        lobbying_spend_annual=influence.get("lobbying_total", 0.0),
        lobbying_trend=lobbying_trend,
        top_issues=top_issues,
        influence_loops=loops_detected,
        suspicion_score=suspicion,
        hypocrisy_flags=hypocrisy_count,
        export_control_risk=export_risk,
        regulatory_actions=regulatory_count,
        analysis_narrative="",  # placeholder
        last_analyzed=now,
        confidence=_compute_confidence(gov, influence, insider, export),
    )

    # Generate narrative (LLM or fallback)
    profile.analysis_narrative = _generate_narrative(engine, ticker, profile)

    # Store in DB
    _store_profile(engine, profile)

    log.info(
        "Company analysis complete: {t} — suspicion={s:.3f}, confidence={c:.2f}",
        t=ticker, s=profile.suspicion_score, c=profile.confidence,
    )

    return profile


def run_analysis_queue(engine: Engine, batch_size: int = 5) -> dict[str, Any]:
    """Process the next batch of unanalyzed companies from ANALYSIS_QUEUE.

    Skips companies already analyzed within the last 30 days.
    Designed to be called by hermes operator or LLM task queue.

    Parameters:
        engine: SQLAlchemy engine.
        batch_size: Number of companies to analyze per cycle (default: 5).

    Returns:
        Summary dict with analyzed tickers, skipped count, errors.
    """
    ensure_table(engine)
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    # Find already-analyzed tickers
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ticker FROM company_profiles "
                "WHERE last_analyzed >= :cutoff"
            ),
            {"cutoff": cutoff},
        ).fetchall()
    recently_analyzed = {row[0] for row in rows}

    # Find next batch to analyze
    to_analyze = []
    for ticker in ANALYSIS_QUEUE:
        if ticker not in recently_analyzed:
            to_analyze.append(ticker)
        if len(to_analyze) >= batch_size:
            break

    if not to_analyze:
        log.info("Company analysis queue: all {n} companies up to date", n=len(ANALYSIS_QUEUE))
        return {
            "analyzed": [],
            "skipped": len(recently_analyzed),
            "remaining": 0,
            "errors": [],
        }

    analyzed = []
    errors = []

    for ticker in to_analyze:
        try:
            profile = analyze_company(engine, ticker)
            analyzed.append(ticker)
        except Exception as exc:
            log.warning("Failed to analyze {t}: {e}", t=ticker, e=str(exc))
            errors.append({"ticker": ticker, "error": str(exc)})

    remaining = len(ANALYSIS_QUEUE) - len(recently_analyzed) - len(analyzed)

    log.info(
        "Company analysis batch: analyzed={a}, errors={e}, remaining={r}",
        a=len(analyzed), e=len(errors), r=remaining,
    )

    return {
        "analyzed": analyzed,
        "skipped": len(recently_analyzed),
        "remaining": max(remaining, 0),
        "errors": errors,
    }


def get_all_profiles(engine: Engine) -> list[CompanyProfile]:
    """Return all analyzed company profiles, sorted by suspicion_score descending.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        List of CompanyProfile instances.
    """
    ensure_table(engine)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ticker, name, sector, profile, narrative, "
                "suspicion_score, last_analyzed "
                "FROM company_profiles "
                "ORDER BY suspicion_score DESC"
            )
        ).fetchall()

    profiles = []
    for row in rows:
        try:
            profile_data = row[3] if isinstance(row[3], dict) else json.loads(row[3])
            profile = CompanyProfile(**profile_data)
            profiles.append(profile)
        except Exception as exc:
            log.debug("Failed to deserialize profile for {t}: {e}", t=row[0], e=str(exc))
    return profiles


def find_cross_company_patterns(engine: Engine) -> list[dict[str, Any]]:
    """Detect patterns across all analyzed company profiles.

    Looks for:
    - Defense companies all increasing lobbying in the same quarter
    - Tech companies with insider selling before the same regulation
    - Congress members on committees holding stocks in their jurisdiction
    - Sector-wide suspicion spikes

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        List of pattern dicts with type, description, companies, and severity.
    """
    profiles = get_all_profiles(engine)
    if not profiles:
        return []

    patterns: list[dict[str, Any]] = []

    # Pattern 1: Sector-wide lobbying trends
    sector_lobbying: dict[str, list[CompanyProfile]] = defaultdict(list)
    for p in profiles:
        if p.lobbying_spend_annual > 0:
            sector_lobbying[p.sector].append(p)

    for sector, companies in sector_lobbying.items():
        increasing = [c for c in companies if c.lobbying_trend == "increasing"]
        if len(increasing) >= 3:
            patterns.append({
                "type": "sector_lobbying_surge",
                "severity": "HIGH",
                "description": (
                    f"{len(increasing)} {sector} companies have increasing lobbying spend: "
                    f"{', '.join(c.ticker for c in increasing)}"
                ),
                "companies": [c.ticker for c in increasing],
                "sector": sector,
            })

    # Pattern 2: Cluster insider selling before regulation
    selling_tickers: dict[str, list[CompanyProfile]] = defaultdict(list)
    for p in profiles:
        if p.insider_net_direction == "net_selling" and p.insider_total_value_90d > 100_000:
            selling_tickers[p.sector].append(p)

    for sector, companies in selling_tickers.items():
        if len(companies) >= 3:
            patterns.append({
                "type": "sector_insider_selling",
                "severity": "HIGH",
                "description": (
                    f"{len(companies)} {sector} companies show net insider selling: "
                    f"{', '.join(c.ticker for c in companies)}. "
                    f"Combined value: ${sum(c.insider_total_value_90d for c in companies):,.0f}"
                ),
                "companies": [c.ticker for c in companies],
                "sector": sector,
            })

    # Pattern 3: Committee holders — congress members on oversight
    committee_holdings: dict[str, list[dict]] = defaultdict(list)
    for p in profiles:
        for holder in p.congress_holders:
            committee = holder.get("committee", "")
            if committee:
                committee_holdings[committee].append({
                    "member": holder.get("member", ""),
                    "ticker": p.ticker,
                    "sector": p.sector,
                    "shares_est": holder.get("shares_est", 0),
                })

    for committee, holdings in committee_holdings.items():
        if len(holdings) >= 3:
            total_value = sum(h.get("shares_est", 0) for h in holdings)
            unique_members = len({h["member"] for h in holdings})
            patterns.append({
                "type": "committee_concentrated_holdings",
                "severity": "CRITICAL" if unique_members >= 3 else "HIGH",
                "description": (
                    f"{unique_members} members on {committee} hold positions in "
                    f"{len(holdings)} companies: "
                    f"{', '.join(set(h['ticker'] for h in holdings))}. "
                    f"Estimated total: ${total_value:,.0f}"
                ),
                "companies": list(set(h["ticker"] for h in holdings)),
                "committee": committee,
                "members": unique_members,
            })

    # Pattern 4: High suspicion clusters
    high_suspicion = [p for p in profiles if p.suspicion_score > 0.5]
    if len(high_suspicion) >= 3:
        by_sector: dict[str, list[str]] = defaultdict(list)
        for p in high_suspicion:
            by_sector[p.sector].append(p.ticker)
        for sector, tickers in by_sector.items():
            if len(tickers) >= 2:
                patterns.append({
                    "type": "suspicion_cluster",
                    "severity": "HIGH",
                    "description": (
                        f"{len(tickers)} {sector} companies have suspicion scores > 0.5: "
                        f"{', '.join(tickers)}"
                    ),
                    "companies": tickers,
                    "sector": sector,
                })

    # Pattern 5: Government contract concentration
    gov_by_sector: dict[str, float] = defaultdict(float)
    for p in profiles:
        gov_by_sector[p.sector] += p.gov_contracts_total

    for sector, total in sorted(gov_by_sector.items(), key=lambda x: -x[1])[:5]:
        if total > 1_000_000_000:
            sector_companies = [
                p.ticker for p in profiles
                if p.sector == sector and p.gov_contracts_total > 0
            ]
            patterns.append({
                "type": "gov_contract_concentration",
                "severity": "MEDIUM",
                "description": (
                    f"{sector} sector received ${total:,.0f} in government contracts "
                    f"across {len(sector_companies)} companies: "
                    f"{', '.join(sector_companies)}"
                ),
                "companies": sector_companies,
                "sector": sector,
                "total_value": total,
            })

    # Sort by severity
    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    patterns.sort(key=lambda p: severity_order.get(p.get("severity", "LOW"), 3))

    log.info("Cross-company patterns: {n} detected", n=len(patterns))
    return patterns


def generate_sector_influence_report(engine: Engine, sector: str) -> str:
    """Generate an LLM narrative summarizing influence across a sector.

    Parameters:
        engine: SQLAlchemy engine.
        sector: Sector name (e.g. 'Technology', 'Semiconductors').

    Returns:
        LLM-generated narrative string.
    """
    profiles = get_all_profiles(engine)
    sector_profiles = [p for p in profiles if p.sector.lower() == sector.lower()]

    if not sector_profiles:
        return f"No analyzed companies found in the {sector} sector."

    # Build summary data for the LLM
    total_lobbying = sum(p.lobbying_spend_annual for p in sector_profiles)
    total_contracts = sum(p.gov_contracts_total for p in sector_profiles)
    total_holders = sum(len(p.congress_holders) for p in sector_profiles)
    avg_suspicion = (
        sum(p.suspicion_score for p in sector_profiles) / len(sector_profiles)
    )
    high_suspicion = [p for p in sector_profiles if p.suspicion_score > 0.3]
    net_selling = [p for p in sector_profiles if p.insider_net_direction == "net_selling"]

    summary = (
        f"Sector: {sector}\n"
        f"Companies analyzed: {len(sector_profiles)}\n"
        f"Total lobbying spend: ${total_lobbying:,.0f}\n"
        f"Total government contracts: ${total_contracts:,.0f}\n"
        f"Congressional holders: {total_holders} positions\n"
        f"Average suspicion score: {avg_suspicion:.3f}\n"
        f"High-suspicion companies: {', '.join(p.ticker for p in high_suspicion) or 'None'}\n"
        f"Companies with net insider selling: {', '.join(p.ticker for p in net_selling) or 'None'}\n\n"
        f"Company-level detail:\n"
    )
    for p in sorted(sector_profiles, key=lambda x: -x.suspicion_score):
        summary += (
            f"  {p.ticker}: suspicion={p.suspicion_score:.3f}, "
            f"lobbying=${p.lobbying_spend_annual:,.0f}, "
            f"contracts=${p.gov_contracts_total:,.0f}, "
            f"insider={p.insider_net_direction}\n"
        )

    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.REASON)
        if client is None or not client.is_available:
            return f"The {sector} sector influence picture:\n\n{summary}"

        result = client.chat(
            [
                {"role": "system", "content": (
                    "You are a financial intelligence analyst at GRID. "
                    "Write a 3-4 paragraph sector influence report. "
                    "Be specific, cite numbers, name companies. No caveats."
                )},
                {"role": "user", "content": (
                    f"Write a sector influence report based on this data:\n\n{summary}"
                )},
            ],
            temperature=0.4,
            num_predict=1000,
        )
        return result or f"The {sector} sector influence picture:\n\n{summary}"
    except Exception:
        return f"The {sector} sector influence picture:\n\n{summary}"


# ══════════════════════════════════════════════════════════════════════════
# INTERNAL — scoring & storage
# ══════════════════════════════════════════════════════════════════════════


def _compute_company_suspicion(
    gov_total: float,
    lobbying_total: float,
    pac_total: float,
    circular_detected: bool,
    hypocrisy_count: int,
    committee_overlap: list[dict],
    insider_direction: str,
    total_money: float,
) -> float:
    """Compute a 0-1 suspicion score for a company's influence profile.

    Factors:
        - Lobbying + government contracts (pay-to-play)     0.20
        - Circular flow detected                            0.20
        - Hypocrisy flags (vote vs trade misalignment)      0.15
        - Committee overlap (holders on oversight committees) 0.15
        - Scale of money in the influence system            0.15
        - Insider selling while lobbying increases           0.15
    """
    score = 0.0

    # Factor 1: Pay-to-play — lobbied AND received contracts
    if lobbying_total > 0 and gov_total > 0:
        ratio = gov_total / max(lobbying_total, 1)
        score += min(0.20, 0.04 * min(ratio, 5))

    # Factor 2: Circular flow
    if circular_detected:
        score += 0.20

    # Factor 3: Hypocrisy flags
    if hypocrisy_count > 0:
        score += min(0.15, 0.05 * hypocrisy_count)

    # Factor 4: Committee overlap
    overlap_with_committee = sum(1 for h in committee_overlap if h.get("committee"))
    if overlap_with_committee > 0:
        score += min(0.15, 0.05 * overlap_with_committee)

    # Factor 5: Scale of money
    if total_money > 100_000_000:
        score += 0.15
    elif total_money > 10_000_000:
        score += 0.10
    elif total_money > 1_000_000:
        score += 0.05

    # Factor 6: Insider selling + lobbying increasing
    if insider_direction == "net_selling" and lobbying_total > 0:
        score += 0.15

    return round(min(score, 1.0), 3)


def _compute_confidence(
    gov: dict, influence: dict, insider: dict, export: dict,
) -> float:
    """Compute confidence in the analysis (0-1) based on data availability."""
    score = 0.0
    checks = 0

    # Gov contracts data present
    checks += 1
    if gov.get("count", 0) > 0:
        score += 1.0

    # Influence data present
    checks += 1
    if influence.get("lobbying_total", 0) > 0 or influence.get("pac_total", 0) > 0:
        score += 1.0

    # Insider data present
    checks += 1
    if insider.get("direction") != "neutral" or len(insider.get("holders", [])) > 0:
        score += 1.0

    # Export data present
    checks += 1
    if export.get("risk_level") not in ("UNKNOWN", None):
        score += 1.0

    # Member trades present
    checks += 1
    if len(influence.get("member_trades", [])) > 0:
        score += 1.0

    return round(score / max(checks, 1), 2)


def _store_profile(engine: Engine, profile: CompanyProfile) -> None:
    """Persist a CompanyProfile to the database (upsert)."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO company_profiles "
                "(ticker, name, sector, profile, narrative, suspicion_score, last_analyzed) "
                "VALUES (:ticker, :name, :sector, :profile, :narrative, :suspicion, :analyzed) "
                "ON CONFLICT (ticker) DO UPDATE SET "
                "name = EXCLUDED.name, "
                "sector = EXCLUDED.sector, "
                "profile = EXCLUDED.profile, "
                "narrative = EXCLUDED.narrative, "
                "suspicion_score = EXCLUDED.suspicion_score, "
                "last_analyzed = EXCLUDED.last_analyzed"
            ),
            {
                "ticker": profile.ticker,
                "name": profile.name,
                "sector": profile.sector,
                "profile": json.dumps(profile.to_dict()),
                "narrative": profile.analysis_narrative,
                "suspicion": profile.suspicion_score,
                "analyzed": profile.last_analyzed,
            },
        )
