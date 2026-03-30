"""
GRID Source Trust Configuration.

Every data source gets a trust tier and base score. The Bayesian trust
scorer updates these over time based on accuracy, but this is the prior.

Trust tiers for API consumers:
    CANONICAL    (0.95-1.0)  Government source of record
    INSTITUTIONAL (0.85-0.95) Verified journalism/research
    COMMERCIAL   (0.70-0.85) Paid data providers
    DERIVED      (0.50-0.70) Community analysis, scraped data
    SOCIAL       (0.20-0.50) Social media, forums
    RUMORED      (0.05-0.20) Anonymous, unverified, dark web

API response color coding:
    GREEN  = confirmed (0.85+)
    YELLOW = estimated (0.50-0.85)
    ORANGE = rumored (0.20-0.50)
    RED    = unverified (0.05-0.20)
"""

SOURCE_TRUST = {
    # ── CANONICAL (0.95-1.0) — Government source of record ──────────
    "sec_edgar": {"tier": "canonical", "base_trust": 0.99, "label": "SEC EDGAR", "color": "green"},
    "sec_form4": {"tier": "canonical", "base_trust": 0.99, "label": "SEC Form 4 Insider", "color": "green"},
    "sec_13f": {"tier": "canonical", "base_trust": 0.99, "label": "SEC 13F Holdings", "color": "green"},
    "sec_companyfacts": {"tier": "canonical", "base_trust": 0.99, "label": "SEC XBRL Financials", "color": "green"},
    "fec": {"tier": "canonical", "base_trust": 0.98, "label": "FEC Campaign Finance", "color": "green"},
    "fara": {"tier": "canonical", "base_trust": 0.98, "label": "DOJ FARA Foreign Agents", "color": "green"},
    "ofac_sdn": {"tier": "canonical", "base_trust": 0.99, "label": "OFAC Sanctions List", "color": "green"},
    "treasury": {"tier": "canonical", "base_trust": 0.99, "label": "US Treasury", "color": "green"},
    "fred": {"tier": "canonical", "base_trust": 0.99, "label": "Federal Reserve FRED", "color": "green"},
    "bls": {"tier": "canonical", "base_trust": 0.98, "label": "Bureau of Labor Statistics", "color": "green"},
    "census": {"tier": "canonical", "base_trust": 0.98, "label": "US Census Bureau", "color": "green"},
    "eia": {"tier": "canonical", "base_trust": 0.99, "label": "Energy Information Admin", "color": "green"},
    "cftc_cot": {"tier": "canonical", "base_trust": 0.99, "label": "CFTC Commitments of Traders", "color": "green"},
    "fda": {"tier": "canonical", "base_trust": 0.99, "label": "FDA Drug Approvals", "color": "green"},
    "usaspending": {"tier": "canonical", "base_trust": 0.98, "label": "USASpending.gov Contracts", "color": "green"},
    "senate_disclosure": {"tier": "canonical", "base_trust": 0.97, "label": "Senate Stock Disclosures", "color": "green"},
    "house_disclosure": {"tier": "canonical", "base_trust": 0.97, "label": "House Stock Disclosures", "color": "green"},
    "congress_votes": {"tier": "canonical", "base_trust": 0.99, "label": "Congressional Votes", "color": "green"},
    "clinicaltrials": {"tier": "canonical", "base_trust": 0.99, "label": "ClinicalTrials.gov", "color": "green"},
    "uspto_patents": {"tier": "canonical", "base_trust": 0.99, "label": "USPTO Patents", "color": "green"},
    "uk_companies_house": {"tier": "canonical", "base_trust": 0.98, "label": "UK Companies House", "color": "green"},
    "eu_sanctions": {"tier": "canonical", "base_trust": 0.99, "label": "EU Sanctions List", "color": "green"},
    "world_bank": {"tier": "canonical", "base_trust": 0.97, "label": "World Bank WDI", "color": "green"},
    "imf": {"tier": "canonical", "base_trust": 0.97, "label": "IMF World Economic Outlook", "color": "green"},
    "bis": {"tier": "canonical", "base_trust": 0.98, "label": "Bank for Intl Settlements", "color": "green"},

    # ── INSTITUTIONAL (0.85-0.95) — Verified journalism/research ────
    "icij": {"tier": "institutional", "base_trust": 0.93, "label": "ICIJ Offshore Leaks", "color": "green"},
    "opensanctions": {"tier": "institutional", "base_trust": 0.92, "label": "OpenSanctions (329 sources)", "color": "green"},
    "opensecrets": {"tier": "institutional", "base_trust": 0.90, "label": "OpenSecrets Lobbying", "color": "green"},
    "littlesis": {"tier": "institutional", "base_trust": 0.88, "label": "LittleSis Power Network", "color": "green"},
    "gleif_lei": {"tier": "institutional", "base_trust": 0.95, "label": "GLEIF Legal Entity IDs", "color": "green"},
    "gdelt": {"tier": "institutional", "base_trust": 0.85, "label": "GDELT Global Events", "color": "green"},
    "reuters": {"tier": "institutional", "base_trust": 0.90, "label": "Reuters Financial News", "color": "green"},
    "opportunity_insights": {"tier": "institutional", "base_trust": 0.92, "label": "Harvard Economic Tracker", "color": "green"},
    "fed_speeches": {"tier": "institutional", "base_trust": 0.95, "label": "Federal Reserve Speeches", "color": "green"},
    "bis_speeches": {"tier": "institutional", "base_trust": 0.95, "label": "BIS Central Bank Speeches", "color": "green"},
    "pbgc": {"tier": "institutional", "base_trust": 0.95, "label": "PBGC Pension Data", "color": "green"},
    "notre_dame_sentiment": {"tier": "institutional", "base_trust": 0.88, "label": "ND SEC Sentiment", "color": "green"},

    # ── COMMERCIAL (0.70-0.85) — Paid data providers ────────────────
    "nansen": {"tier": "commercial", "base_trust": 0.82, "label": "Nansen Wallet Labels", "color": "yellow"},
    "dune": {"tier": "commercial", "base_trust": 0.80, "label": "Dune Analytics On-Chain", "color": "yellow"},
    "glassnode": {"tier": "commercial", "base_trust": 0.82, "label": "Glassnode On-Chain", "color": "yellow"},
    "cryptoquant": {"tier": "commercial", "base_trust": 0.80, "label": "CryptoQuant Flows", "color": "yellow"},
    "arkham": {"tier": "commercial", "base_trust": 0.78, "label": "Arkham Intelligence", "color": "yellow"},
    "bubble_maps": {"tier": "commercial", "base_trust": 0.75, "label": "Bubble Maps Clustering", "color": "yellow"},
    "defilama": {"tier": "commercial", "base_trust": 0.85, "label": "DefiLlama TVL/Yields", "color": "yellow"},
    "openbb": {"tier": "commercial", "base_trust": 0.85, "label": "OpenBB Market Data", "color": "yellow"},
    "crypto_com": {"tier": "commercial", "base_trust": 0.85, "label": "Crypto.com Exchange", "color": "yellow"},
    "binance_data": {"tier": "commercial", "base_trust": 0.85, "label": "Binance Historical", "color": "yellow"},
    "yfinance": {"tier": "commercial", "base_trust": 0.80, "label": "Yahoo Finance", "color": "yellow"},

    # ── DERIVED (0.50-0.70) — Community analysis, scraped data ──────
    "kaggle": {"tier": "derived", "base_trust": 0.55, "label": "Kaggle Dataset", "color": "yellow"},
    "github_repo": {"tier": "derived", "base_trust": 0.50, "label": "GitHub Community Data", "color": "yellow"},
    "huggingface": {"tier": "derived", "base_trust": 0.60, "label": "HuggingFace Dataset", "color": "yellow"},
    "academic_paper": {"tier": "derived", "base_trust": 0.70, "label": "Academic Research", "color": "yellow"},
    "crypto_data_download": {"tier": "derived", "base_trust": 0.65, "label": "CryptoDataDownload", "color": "yellow"},
    "quiver_quant": {"tier": "derived", "base_trust": 0.65, "label": "Quiver Quantitative", "color": "yellow"},
    "finviz": {"tier": "derived", "base_trust": 0.70, "label": "Finviz Screener", "color": "yellow"},
    "wikipedia_pageviews": {"tier": "derived", "base_trust": 0.60, "label": "Wikipedia Pageviews", "color": "yellow"},
    "google_trends": {"tier": "derived", "base_trust": 0.60, "label": "Google Search Trends", "color": "yellow"},
    "glassdoor": {"tier": "derived", "base_trust": 0.55, "label": "Glassdoor Reviews", "color": "yellow"},
    "qwen_research": {"tier": "derived", "base_trust": 0.55, "label": "Qwen 32B Research", "color": "yellow"},
    "claude_research": {"tier": "derived", "base_trust": 0.65, "label": "Claude Research", "color": "yellow"},
    "gpt_research": {"tier": "derived", "base_trust": 0.60, "label": "GPT Research", "color": "yellow"},

    # ── SOCIAL (0.20-0.50) — Social media, forums ──────────────────
    "reddit_wsb": {"tier": "social", "base_trust": 0.30, "label": "Reddit WallStreetBets", "color": "orange"},
    "reddit_stocks": {"tier": "social", "base_trust": 0.35, "label": "Reddit r/stocks", "color": "orange"},
    "reddit_crypto": {"tier": "social", "base_trust": 0.25, "label": "Reddit r/cryptocurrency", "color": "orange"},
    "twitter_fintwit": {"tier": "social", "base_trust": 0.35, "label": "FinTwit", "color": "orange"},
    "twitter_crypto": {"tier": "social", "base_trust": 0.25, "label": "Crypto Twitter", "color": "orange"},
    "stocktwits": {"tier": "social", "base_trust": 0.30, "label": "StockTwits", "color": "orange"},
    "telegram_crypto": {"tier": "social", "base_trust": 0.20, "label": "Telegram Crypto Groups", "color": "orange"},
    "discord_trading": {"tier": "social", "base_trust": 0.20, "label": "Discord Trading", "color": "orange"},
    "youtube_finance": {"tier": "social", "base_trust": 0.25, "label": "YouTube Finance", "color": "orange"},
    "tiktok_finance": {"tier": "social", "base_trust": 0.15, "label": "TikTok Finance", "color": "orange"},

    # ── RUMORED (0.05-0.20) — Anonymous, unverified, dark web ──────
    "4chan_biz": {"tier": "rumored", "base_trust": 0.15, "label": "4chan /biz/", "color": "red"},
    "dark_web": {"tier": "rumored", "base_trust": 0.10, "label": "Dark Web Intel", "color": "red"},
    "anonymous_tip": {"tier": "rumored", "base_trust": 0.10, "label": "Anonymous Tip", "color": "red"},
    "unverified_leak": {"tier": "rumored", "base_trust": 0.15, "label": "Unverified Leak", "color": "red"},
    "conspiracy": {"tier": "rumored", "base_trust": 0.05, "label": "Unverified Claim", "color": "red"},
}


def get_trust(source_key: str) -> dict:
    """Get trust config for a source. Returns default if unknown."""
    return SOURCE_TRUST.get(source_key, {
        "tier": "derived",
        "base_trust": 0.50,
        "label": source_key,
        "color": "yellow",
    })


def trust_color(score: float) -> str:
    """Map trust score to display color."""
    if score >= 0.85:
        return "green"
    if score >= 0.50:
        return "yellow"
    if score >= 0.20:
        return "orange"
    return "red"


def trust_label(score: float) -> str:
    """Map trust score to confidence label."""
    if score >= 0.95:
        return "confirmed"
    if score >= 0.85:
        return "confirmed"
    if score >= 0.70:
        return "derived"
    if score >= 0.50:
        return "estimated"
    if score >= 0.20:
        return "rumored"
    return "inferred"
